"""Idempotent Parquet persistence layer with date-partitioned dynamic upsert."""

import json
import logging
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.config import Config
from app.schemas.alert import ALERT_SCHEMA
from app.schemas.enriched_transaction import STORED_ENRICHED_SCHEMA
from app.schemas.transaction import STORED_RAW_SCHEMA

logger = logging.getLogger("sentinela.storage")


def base_dir(cfg: Config) -> Path:
    """Returns base storage directory path.

    Args:
        cfg: Application configuration container.

    Returns:
        Path object pointing to base data directory.
    """
    return Path(cfg.data_dir)


def raw_path(cfg: Config) -> str:
    """Returns filesystem path string for raw transactions dataset.

    Args:
        cfg: Application configuration container.

    Returns:
        String path to raw storage layer.
    """
    return str(base_dir(cfg) / "raw")


def enriched_path(cfg: Config) -> str:
    """Returns filesystem path string for enriched transactions dataset.

    Args:
        cfg: Application configuration container.

    Returns:
        String path to enriched storage layer.
    """
    return str(base_dir(cfg) / "enriched")


def alerts_path(cfg: Config) -> str:
    """Returns filesystem path string for fraud alerts dataset.

    Args:
        cfg: Application configuration container.

    Returns:
        String path to alerts storage layer.
    """
    return str(base_dir(cfg) / "alerts")


def profiles_path(cfg: Config) -> str:
    """Returns filesystem path string for customer profiles dataset.

    Args:
        cfg: Application configuration container.

    Returns:
        String path to profiles storage layer.
    """
    return str(base_dir(cfg) / "profiles")


def audit_path(cfg: Config) -> str:
    """Returns filesystem path string for audit log dataset.

    Args:
        cfg: Application configuration container.

    Returns:
        String path to audit storage layer.
    """
    return str(base_dir(cfg) / "audit")


def _existing_ids(df: DataFrame, path: str, spark: SparkSession) -> set[str]:
    """Scans existing dataset to extract set of already persisted transaction identifiers.

    Args:
        df: Target DataFrame reference for schema inspection.
        path: Storage path string.
        spark: Active SparkSession.

    Returns:
        Set of existing transaction ID strings.
    """
    from app.storage.reader import read_partitioned

    existing = read_partitioned(spark, path, df.schema)
    if existing is None:
        return set()
    return {str(r[0]) for r in existing.select("transaction_id").distinct().collect()}


def _write_partitioned(df: DataFrame, path: str, spark: SparkSession) -> None:
    """Writes DataFrame to Parquet dataset partitioned by date (dt=yyyy-MM-dd).

    Args:
        df: Spark DataFrame to persist.
        path: Target root directory path.
        spark: Active SparkSession.
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    pdf = df.toPandas()
    if pdf.empty:
        logger.debug("writing_dataset_records count=0 path=%s", path)
        return
    logger.debug("writing_dataset_records count=%d path=%s", len(pdf), path)
    if "timestamp" in pdf.columns:
        ts_s = pd.to_datetime(pdf["timestamp"])
        if hasattr(ts_s.dt, "tz") and ts_s.dt.tz is not None:
            ts_s = ts_s.dt.tz_convert("UTC").dt.tz_localize(None)
        pdf["dt"] = ts_s.dt.strftime("%Y-%m-%d")
        pdf["timestamp"] = ts_s.astype("datetime64[us]")
    if "decision_timestamp" in pdf.columns:
        dts_s = pd.to_datetime(pdf["decision_timestamp"])
        if hasattr(dts_s.dt, "tz") and dts_s.dt.tz is not None:
            dts_s = dts_s.dt.tz_convert("UTC").dt.tz_localize(None)
        pdf["decision_timestamp"] = dts_s.astype("datetime64[us]")

    out_p = Path(path)
    out_p.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(pdf)
    pq.write_to_dataset(
        table, root_path=str(out_p), partition_cols=["dt"], existing_data_behavior="overwrite_or_ignore"
    )


def upsert_raw(batch: DataFrame, cfg: Config, spark: SparkSession) -> None:
    """Performs idempotent append of unique raw transactions.

    Args:
        batch: Micro-batch DataFrame of incoming raw transactions.
        cfg: Application configuration container.
        spark: Active SparkSession.
    """
    df = batch.select(*STORED_RAW_SCHEMA.names)
    path = raw_path(cfg)
    existing = set(_existing_ids(df, path, spark))
    if existing:
        df = df.filter(~F.col("transaction_id").isin(existing))
    if df.count() == 0:
        return
    _write_partitioned(df, path, spark)


def upsert_enriched(enriched: DataFrame, cfg: Config, spark: SparkSession) -> None:
    """Performs idempotent append of unique enriched transactions.

    Args:
        enriched: Micro-batch DataFrame of scored transactions.
        cfg: Application configuration container.
        spark: Active SparkSession.
    """
    df = enriched.select(*STORED_ENRICHED_SCHEMA.names)
    path = enriched_path(cfg)
    existing = set(_existing_ids(df, path, spark))
    if existing:
        df = df.filter(~F.col("transaction_id").isin(existing))
    if df.count() == 0:
        return
    _write_partitioned(df, path, spark)


def upsert_alerts(alerts: DataFrame, cfg: Config, spark: SparkSession) -> None:
    """Performs idempotent append of unique fraud alert events.

    Args:
        alerts: Micro-batch DataFrame of generated alert transactions.
        cfg: Application configuration container.
        spark: Active SparkSession.
    """
    if alerts.count() == 0:
        return
    df = alerts.select(*ALERT_SCHEMA.names)
    path = alerts_path(cfg)
    existing = set(_existing_ids(df, path, spark))
    if existing:
        df = df.filter(~F.col("transaction_id").isin(existing))
    if df.count() == 0:
        return
    _write_partitioned(df, path, spark)


def write_profile(profile_df: DataFrame, cfg: Config) -> None:
    """Persists customer behavioral profiles with deterministic deduplication.

    Args:
        profile_df: DataFrame containing latest customer profile records.
        cfg: Application configuration container.
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = profiles_path(cfg)
    profile_file = Path(path) / "profiles.parquet"
    Path(path).mkdir(parents=True, exist_ok=True)
    pdf = profile_df.toPandas()
    if pdf.empty:
        logger.debug("writing_profile_records count=0 path=%s", path)
        return

    if "last_timestamp" in pdf.columns:
        pdf["last_timestamp"] = pd.to_datetime(pdf["last_timestamp"]).astype("datetime64[us]")
    if "updated_at" in pdf.columns:
        pdf["updated_at"] = pd.to_datetime(pdf["updated_at"]).astype("datetime64[us]")

    if profile_file.exists():
        try:
            existing_pdf = pq.read_table(str(profile_file)).to_pandas()
            if "last_timestamp" in existing_pdf.columns:
                existing_pdf["last_timestamp"] = pd.to_datetime(existing_pdf["last_timestamp"]).astype("datetime64[us]")
            if "updated_at" in existing_pdf.columns:
                existing_pdf["updated_at"] = pd.to_datetime(existing_pdf["updated_at"]).astype("datetime64[us]")
            pdf = pd.concat([existing_pdf, pdf], ignore_index=True)
            pdf = pdf.sort_values("last_timestamp").groupby("card_id", as_index=False).last()
        except (OSError, ValueError, TypeError) as exc:
            logger.warning("profile_merge_fallback path=%s error=%s", profile_file, exc)

    logger.debug("writing_profile_records count=%d path=%s", len(pdf), path)
    table = pa.Table.from_pandas(pdf)
    pq.write_table(table, str(profile_file))


def append_audit(json_rows: list[dict[str, Any]], cfg: Config) -> None:
    """Appends audit trail records to JSONL storage file.

    Args:
        json_rows: List of audit event dictionaries.
        cfg: Application configuration container.
    """
    out_dir = Path(audit_path(cfg))
    out_dir.mkdir(parents=True, exist_ok=True)
    file_path = out_dir / "decisions.jsonl"
    with file_path.open("a", encoding="utf-8") as fh:
        for row in json_rows:
            fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def to_json_snapshot(d: dict[str, Any]) -> str:
    """Serializes feature dictionary into JSON snapshot string.

    Args:
        d: Dictionary containing scalar or array features.

    Returns:
        JSON formatted string snapshot.
    """
    cleaned: dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            cleaned[k] = None
        elif hasattr(v, "item"):
            cleaned[k] = v.item()
        else:
            cleaned[k] = v
    return json.dumps(cleaned, ensure_ascii=False, default=str)
