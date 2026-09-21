"""Fraud pipeline orchestrator: feature extraction, rule scoring, decisioning, and idempotent persistence."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, Row, SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from app.config import Config
from app.features.geographic import city_distance_km, implicit_speed_kmh
from app.features.merchant_risk import is_high_risk_merchant
from app.features.spark import attach_spark_features
from app.rules.engine import build_rules, evaluate_all
from app.schemas.alert import ALERT_SCHEMA
from app.schemas.enriched_transaction import ENRICHED_SCHEMA
from app.scoring.combiner import combine
from app.scoring.threshold import decide
from app.storage.reader import read_raw
from app.storage.writer import to_json_snapshot, upsert_alerts, upsert_enriched, upsert_raw, write_profile

RAW_KEYS: list[str] = [
    "transaction_id",
    "customer_id",
    "card_id",
    "timestamp",
    "amount",
    "currency",
    "merchant_id",
    "merchant_category",
    "merchant_city",
    "merchant_country",
    "terminal_id",
    "entry_mode",
    "device_id",
    "channel",
    "auth_result",
]

FEATURE_KEYS: list[str] = [
    "velocity_1min",
    "velocity_5min",
    "velocity_1h",
    "velocity_24h",
    "count_10min",
    "sum_10min",
    "min_10min",
    "amount_zscore",
    "amount_vs_avg_ratio",
    "distance_from_last_km",
    "travel_speed_kmh",
    "minutes_since_last",
    "dormant_days",
    "new_terminal",
    "new_device",
    "new_city",
    "entry_mode_chip_only_history",
    "high_risk_merchant",
    "hour_of_day",
    "bin_denials_15min",
    "previous_transaction_id",
]

DECISION_KEYS: list[str] = [
    "risk_score",
    "risk_level",
    "triggered_rules",
    "rule_reasons",
    "decision_timestamp",
    "model_version",
]

PROFILE_SCHEMA = StructType(
    [
        StructField("card_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("last_timestamp", TimestampType(), True),
        StructField("last_city", StringType(), True),
        StructField("last_terminal", StringType(), True),
        StructField("last_device", StringType(), True),
        StructField("last_amount", DoubleType(), True),
        StructField("risk_score", DoubleType(), True),
        StructField("risk_level", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


@dataclass
class PipelineResult:
    """Container holding batch processing outputs and aggregated execution metrics."""

    enriched: DataFrame
    alerts: DataFrame
    stats: dict[str, Any] = field(default_factory=dict)


class FraudPipeline:
    """Core pipeline coordinating feature enrichment, rule evaluation, and storage persistence."""

    def __init__(self, spark: SparkSession, cfg: Config, write: bool = True) -> None:
        """Initializes pipeline with Spark context and active rules.

        Args:
            spark: Active SparkSession.
            cfg: Application configuration container.
            write: Whether to persist outputs to Parquet storage layers (default: True).
        """
        self.spark = spark
        self.cfg = cfg
        self.write = write
        self.rules = build_rules(cfg)
        self._snapshots: dict[str, str] = {}

    def enrich(self, batch: DataFrame, history: DataFrame | None = None) -> DataFrame:
        """Enriches raw transaction micro-batch with window features and evaluated rule decisions.

        Args:
            batch: Micro-batch DataFrame of incoming raw transactions.
            history: Optional historical DataFrame reference.

        Returns:
            Enriched Spark DataFrame matching ENRICHED_SCHEMA.
        """
        if history is None:
            history = read_raw(self.cfg, self.spark)
        if history is not None and history.schema:
            batch_ids = [str(r[0]) for r in batch.select("transaction_id").distinct().collect()]
            if batch_ids:
                history = history.filter(~F.col("transaction_id").isin(batch_ids))
        tx_features_df = attach_spark_features(batch, history, self.spark)
        rows = tx_features_df.collect()

        enriched_rows: list[dict[str, Any]] = []
        self._snapshots = {}
        decision_ts = datetime.now(timezone.utc).replace(tzinfo=None)
        max_score = float(self.cfg.scoring.get("max_score", 100))
        for row in rows:
            tx, f = self._split(row)
            results = evaluate_all(self.rules, tx, f)
            score, triggered, reasons = combine(results, max_score)
            level = decide(score, self.cfg.thresholds)
            self._snapshots[tx["transaction_id"]] = to_json_snapshot({**f, "rule_reasons": reasons, "risk_score": round(float(score), 2)})
            enriched_rows.append(
                {
                    "transaction_id": tx["transaction_id"],
                    "customer_id": tx["customer_id"],
                    "card_id": tx["card_id"],
                    "timestamp": tx["timestamp"],
                    "amount": float(tx["amount"] or 0.0),
                    "currency": tx["currency"],
                    "merchant_id": tx["merchant_id"],
                    "merchant_category": tx["merchant_category"],
                    "merchant_city": tx["merchant_city"],
                    "merchant_country": tx["merchant_country"],
                    "terminal_id": tx["terminal_id"],
                    "entry_mode": tx["entry_mode"],
                    "device_id": tx["device_id"],
                    "channel": tx["channel"],
                    "auth_result": tx["auth_result"],
                    "velocity_1min": self._int(f["velocity_1min"]),
                    "velocity_5min": self._int(f["velocity_5min"]),
                    "velocity_1h": self._int(f["velocity_1h"]),
                    "velocity_24h": self._int(f["velocity_24h"]),
                    "count_10min": self._int(f["count_10min"]),
                    "sum_10min": round(float(f["sum_10min"] or 0.0), 2),
                    "min_10min": round(float(f["min_10min"] or 0.0), 2),
                    "amount_zscore": round(float(f["amount_zscore"] or 0.0), 3),
                    "amount_vs_avg_ratio": round(float(f["amount_vs_avg_ratio"] or 1.0), 3),
                    "distance_from_last_km": round(float(f.get("distance_from_last_km") or 0.0), 3),
                    "travel_speed_kmh": round(float(f.get("travel_speed_kmh") or 0.0), 3),
                    "minutes_since_last": round(float(f.get("minutes_since_last") or 0.0), 3),
                    "dormant_days": round(float(f.get("dormant_days") or 0.0), 3),
                    "new_terminal": bool(f.get("new_terminal")),
                    "new_device": bool(f.get("new_device")),
                    "new_city": bool(f.get("new_city")),
                    "entry_mode_chip_only_history": bool(f.get("entry_mode_chip_only_history")),
                    "high_risk_merchant": bool(f.get("high_risk_merchant")),
                    "hour_of_day": self._int(f.get("hour_of_day")),
                    "bin_denials_15min": self._int(f.get("bin_denials_15min")),
                    "previous_transaction_id": f.get("previous_transaction_id"),
                    "risk_score": round(float(score), 2),
                    "risk_level": level,
                    "triggered_rules": triggered,
                    "rule_reasons": reasons,
                    "decision_timestamp": decision_ts,
                    "model_version": self.cfg.model_version,
                }
            )

        if not enriched_rows:
            return self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema=ENRICHED_SCHEMA)
        import pandas as pd

        return self.spark.createDataFrame(pd.DataFrame(enriched_rows), schema=ENRICHED_SCHEMA)

    def process_batch(self, batch: DataFrame) -> PipelineResult:
        """Executes full micro-batch lifecycle: enrich, filter alerts, and persist layers.

        Args:
            batch: Micro-batch DataFrame of incoming transactions.

        Returns:
            PipelineResult containing enriched DataFrame, alerts DataFrame, and execution statistics.
        """
        enriched = self.enrich(batch)
        alerts = self._build_alerts(enriched)
        if self.write:
            upsert_raw(batch, self.cfg, self.spark)
            upsert_enriched(enriched, self.cfg, self.spark)
            upsert_alerts(alerts, self.cfg, self.spark)
            self._log_to_audit(enriched)
            profile = self._build_profile(enriched)
            if profile is not None:
                write_profile(profile, self.cfg)
        return PipelineResult(enriched=enriched, alerts=alerts, stats=self._stats(enriched, alerts))

    def _split(self, row: Row) -> tuple[dict[str, Any], dict[str, Any]]:
        """Separates row into raw transaction fields and computed behavioral features.

        Args:
            row: PySpark Row from feature-attached DataFrame.

        Returns:
            Tuple of (transaction_attributes_dict, computed_features_dict).
        """
        tx: dict[str, Any] = {
            "transaction_id": row["transaction_id"],
            "customer_id": row["customer_id"],
            "card_id": row["card_id"],
            "timestamp": row["ts"],
            "amount": float(row["amount"] or 0.0),
            "currency": row["currency"],
            "merchant_id": row["merchant_id"],
            "merchant_category": row["merchant_category"],
            "merchant_city": row["merchant_city"],
            "merchant_country": row["merchant_country"],
            "terminal_id": row["terminal_id"],
            "entry_mode": row["entry_mode"],
            "device_id": row["device_id"],
            "channel": row["channel"],
            "auth_result": row["auth_result"],
        }
        ts = tx["timestamp"]
        prev_ts = row["prev_ts"]
        minutes = None
        if prev_ts is not None and ts is not None:
            minutes = max(0.0, (ts - prev_ts).total_seconds() / 60.0)
        distance = city_distance_km(row["prev_merchant_city"], tx["merchant_city"])
        speed = implicit_speed_kmh(distance, minutes) if minutes is not None and minutes > 0 else 0.0
        dormant = 0.0
        if prev_ts is not None and ts is not None:
            dormant = (ts - prev_ts).total_seconds() / 86400.0

        mean = float(row["amount_mean_90d"] or 0.0)
        std = float(row["amount_std_90d"] or 0.0)
        zscore = (tx["amount"] - mean) / std if (mean > 0 and std and 0 < std) else 0.0
        ratio = tx["amount"] / mean if mean > 0 else 1.0
        hour = ts.hour if ts is not None else 0

        f: dict[str, Any] = {
            "velocity_1min": self._py(row["velocity_1min"]),
            "velocity_5min": self._py(row["velocity_5min"]),
            "velocity_1h": self._py(row["velocity_1h"]),
            "velocity_24h": self._py(row["velocity_24h"]),
            "count_10min": self._py(row["count_10min"]),
            "sum_10min": self._py(row["sum_10min"]),
            "min_10min": self._py(row["min_10min"]),
            "amount_mean_90d": mean,
            "amount_count_90d": self._int(row["amount_count_90d"]),
            "prev_ts": prev_ts,
            "prev_merchant_city": row["prev_merchant_city"],
            "prev_terminal_id": row["prev_terminal_id"],
            "prev_device_id": row["prev_device_id"],
            "prev_amount": row["prev_amount"],
            "prev_entry_mode": row["prev_entry_mode"],
            "previous_transaction_id": row["prev_transaction_id"],
            "bin_denials_15min": self._int(row["bin_denials_15min"]),
            "prev_bin_approved": bool(row["prev_bin_approved"]),
            "new_terminal": bool(row["new_terminal"]),
            "new_device": bool(row["new_device"]),
            "new_city": bool(row["new_city"]),
            "entry_mode_chip_only_history": bool(row["entry_mode_chip_only_history"]),
            "distance_from_last_km": distance,
            "travel_speed_kmh": speed,
            "minutes_since_last": minutes,
            "dormant_days": dormant,
            "amount_zscore": zscore,
            "amount_vs_avg_ratio": ratio,
            "hour_of_day": hour,
            "high_risk_merchant": is_high_risk_merchant(tx.get("merchant_category"), self.cfg.high_risk_categories()),
        }
        return tx, f

    def _build_alerts(self, enriched: DataFrame) -> DataFrame:
        """Filters enriched transactions to build review/block alerts DataFrame.

        Args:
            enriched: Enriched transactions DataFrame.

        Returns:
            Alerts DataFrame matching ALERT_SCHEMA.
        """
        rows = enriched.collect()
        alert_rows: list[dict[str, Any]] = []
        for r in rows:
            d = r.asDict()
            if d.get("risk_level") not in ("review", "block"):
                continue
            alert_rows.append(
                {
                    "transaction_id": d["transaction_id"],
                    "customer_id": d["customer_id"],
                    "card_id": d["card_id"],
                    "bin": str(d["card_id"])[:6],
                    "timestamp": d["timestamp"],
                    "amount": round(float(d.get("amount") or 0.0), 2),
                    "currency": d.get("currency"),
                    "merchant_id": d.get("merchant_id"),
                    "merchant_category": d.get("merchant_category"),
                    "merchant_city": d.get("merchant_city"),
                    "merchant_country": d.get("merchant_country"),
                    "entry_mode": d.get("entry_mode"),
                    "channel": d.get("channel"),
                    "risk_score": float(d.get("risk_score") or 0.0),
                    "risk_level": d["risk_level"],
                    "triggered_rules": list(d.get("triggered_rules") or []),
                    "rule_reasons": list(d.get("rule_reasons") or []),
                    "feature_snapshot": self._snapshots.get(d["transaction_id"], "{}"),
                    "decision_timestamp": d.get("decision_timestamp"),
                    "model_version": d.get("model_version"),
                }
            )
        if not alert_rows:
            return self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema=ALERT_SCHEMA)
        import pandas as pd

        return self.spark.createDataFrame(pd.DataFrame(alert_rows), schema=ALERT_SCHEMA)

    def _build_profile(self, enriched: DataFrame) -> DataFrame | None:
        """Constructs latest customer profile state DataFrame from enriched batch.

        Args:
            enriched: Enriched transactions DataFrame.

        Returns:
            Profiles DataFrame matching PROFILE_SCHEMA or None.
        """
        if enriched.count() == 0:
            return None
        pdf = enriched.toPandas()
        if pdf.empty:
            return None
        pdf_sorted = pdf.sort_values("timestamp").groupby("card_id", as_index=False).last()
        now_ts = datetime.now(timezone.utc).replace(tzinfo=None)
        profile: list[dict[str, Any]] = []
        for _, r in pdf_sorted.iterrows():
            profile.append(
                {
                    "card_id": r["card_id"],
                    "customer_id": r["customer_id"],
                    "last_timestamp": r["timestamp"],
                    "last_city": r["merchant_city"],
                    "last_terminal": r["terminal_id"],
                    "last_device": r["device_id"],
                    "last_amount": float(r["amount"] or 0),
                    "risk_score": float(r["risk_score"] or 0),
                    "risk_level": r["risk_level"],
                    "updated_at": now_ts,
                }
            )
        import pandas as pd

        return self.spark.createDataFrame(pd.DataFrame(profile), schema=PROFILE_SCHEMA)

    def _log_to_audit(self, enriched: DataFrame) -> None:
        """Logs enriched batch decisions to audit trail.

        Args:
            enriched: Enriched transactions DataFrame.
        """
        from app.audit.logger import log_decisions

        log_decisions(enriched, self.cfg)

    def _stats(self, enriched: DataFrame, alerts: DataFrame) -> dict[str, Any]:
        """Calculates aggregated metrics over processed batch.

        Args:
            enriched: Enriched transactions DataFrame.
            alerts: Generated alerts DataFrame.

        Returns:
            Dictionary with counts and average risk score.
        """
        rows = enriched.collect()
        counts: dict[str, int] = {"approve": 0, "review": 0, "block": 0}
        triggered: dict[str, int] = {}
        scores: list[float] = []
        for r in rows:
            d = r.asDict()
            counts[d["risk_level"]] = counts.get(d["risk_level"], 0) + 1
            scores.append(float(d.get("risk_score") or 0))
            for rule in d.get("triggered_rules") or []:
                triggered[rule] = triggered.get(rule, 0) + 1
        return {
            "processed": len(rows),
            "alerts": alerts.count(),
            **counts,
            "triggered_by_rule": triggered,
            "avg_score": round(sum(scores) / len(scores), 2) if scores else 0.0,
        }

    @staticmethod
    def _py(v: Any) -> Any:
        """Unwraps numpy scalar values into python native types."""
        if hasattr(v, "item"):
            return v.item()
        return v

    @staticmethod
    def _int(v: Any) -> int:
        """Converts nullable or numpy values safely into python int."""
        if v is None:
            return 0
        if hasattr(v, "item"):
            v = v.item()
        return int(v)