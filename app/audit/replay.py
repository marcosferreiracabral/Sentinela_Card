"""Transaction audit replay and idempotency verification engine."""

from typing import Any

from pyspark.sql import SparkSession, functions as F

from app.config import Config
from app.storage.reader import read_enriched


class RecordNotFound(KeyError):
    """Raised when target transaction record is missing from data storage layers."""


def replay_transaction(spark: SparkSession, cfg: Config, transaction_id: str) -> dict[str, Any]:
    """Retrieves stored decision and calculated features for a specific transaction.

    Args:
        spark: Active SparkSession.
        cfg: Application configuration container.
        transaction_id: Unique transaction identifier.

    Returns:
        Dictionary containing stored transaction fields and rule results.

    Raises:
        RecordNotFound: If transaction is not found in enriched dataset.
    """
    enriched = read_enriched(cfg, spark)
    if enriched is None:
        raise RecordNotFound(f"transaction {transaction_id} not found in enriched dataset")
    match = enriched.filter(F.col("transaction_id") == transaction_id)
    rows = match.toPandas()
    if rows.empty:
        raise RecordNotFound(f"transaction {transaction_id} not found in enriched dataset")
    row = rows.iloc[0]
    value = row.to_dict()
    for k, v in value.items():
        if k in ("triggered_rules", "rule_reasons"):
            if v is None:
                value[k] = []
            elif hasattr(v, "tolist"):
                value[k] = v.tolist()
            elif isinstance(v, list):
                value[k] = v
            elif isinstance(v, str):
                value[k] = [v] if v else []
            elif hasattr(v, "__iter__"):
                value[k] = list(v)
            else:
                value[k] = [v]
        elif hasattr(v, "item"):
            try:
                value[k] = v.item()
            except (ValueError, TypeError):
                value[k] = list(v)
    return value


def recompute_decision(spark: SparkSession, cfg: Config, transaction_id: str) -> dict[str, Any]:
    """Recomputes decision for a single transaction against prior historical state without persisting.

    Args:
        spark: Active SparkSession.
        cfg: Application configuration container.
        transaction_id: Unique transaction identifier.

    Returns:
        Dictionary containing freshly computed decision attributes.

    Raises:
        RecordNotFound: If transaction is not found in raw dataset.
    """
    from app.schemas.transaction import TRANSACTION_SCHEMA
    from app.storage.reader import read_raw
    from app.streaming.processor import FraudPipeline, RAW_KEYS

    raw = read_raw(cfg, spark)
    if raw is None:
        raise RecordNotFound(f"transaction {transaction_id} not found in raw dataset")
    row = raw.filter(F.col("transaction_id") == transaction_id).toPandas()
    if row.empty:
        raise RecordNotFound(f"transaction {transaction_id} not found in raw dataset")
    tx_ts = row.iloc[0]["timestamp"]
    prior_history = raw.filter(
        (F.col("timestamp") < tx_ts)
        | ((F.col("timestamp") == tx_ts) & (F.col("transaction_id") != transaction_id))
    )

    cols = [k for k in RAW_KEYS if k in row.columns]
    batch = spark.createDataFrame(row[cols], schema=TRANSACTION_SCHEMA)
    pipeline = FraudPipeline(spark, cfg, write=False)
    enriched = pipeline.enrich(batch, history=prior_history)
    return enriched.toPandas().iloc[0].to_dict()


def _to_list(val: Any) -> list[Any]:
    """Normalizes input values into python list.

    Args:
        val: Input scalar, list, or array-like object.

    Returns:
        Standard python list representation.
    """
    if val is None:
        return []
    if isinstance(val, str):
        return [val] if val else []
    if hasattr(val, "tolist"):
        return val.tolist()
    if hasattr(val, "__len__"):
        return list(val)
    return [val]


def verify_replay(spark: SparkSession, cfg: Config, transaction_id: str) -> dict[str, Any]:
    """Compares persisted transaction decision against fresh recomputation.

    Args:
        spark: Active SparkSession.
        cfg: Application configuration container.
        transaction_id: Unique transaction identifier.

    Returns:
        Dictionary summarizing replay verification verdict and comparison.
    """
    stored = replay_transaction(spark, cfg, transaction_id)
    recomputed = recompute_decision(spark, cfg, transaction_id)
    return {
        "transaction_id": transaction_id,
        "stored_level": stored.get("risk_level"),
        "recomputed_level": recomputed.get("risk_level"),
        "stored_score": round(float(stored.get("risk_score") or 0), 2),
        "recomputed_score": round(float(recomputed.get("risk_score") or 0), 2),
        "stored_rules": _to_list(stored.get("triggered_rules")),
        "recomputed_rules": _to_list(recomputed.get("triggered_rules")),
        "idempotent": stored.get("risk_level") == recomputed.get("risk_level")
        and stored.get("risk_score") == recomputed.get("risk_score"),
    }