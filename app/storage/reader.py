"""Parquet storage layer reader for raw, enriched, alert, and customer profile datasets."""

import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from app.config import Config
from app.schemas.alert import ALERT_SCHEMA
from app.schemas.enriched_transaction import STORED_ENRICHED_SCHEMA
from app.schemas.transaction import STORED_RAW_SCHEMA
from app.storage.writer import alerts_path, audit_path, enriched_path, profiles_path, raw_path


def read_partitioned(spark: SparkSession, path: str, schema: StructType | None = None) -> DataFrame | None:
    """Reads partitioned Parquet files from directory tree.

    Args:
        spark: Active SparkSession.
        path: Root directory path of target dataset.
        schema: Optional StructType schema to enforce.

    Returns:
        Spark DataFrame or None if path/files do not exist.
    """
    p = Path(path)
    if not p.exists():
        return None
    files = [str(f) for f in p.rglob("*.parquet")]
    if not files:
        return None
    return spark.read.parquet(*files)


def read_raw(cfg: Config, spark: SparkSession) -> DataFrame | None:
    """Reads persisted raw transactions dataset.

    Args:
        cfg: Application configuration container.
        spark: Active SparkSession.

    Returns:
        Spark DataFrame of raw transactions or None.
    """
    return read_partitioned(spark, raw_path(cfg), STORED_RAW_SCHEMA)


def read_enriched(cfg: Config, spark: SparkSession) -> DataFrame | None:
    """Reads persisted enriched transactions dataset.

    Args:
        cfg: Application configuration container.
        spark: Active SparkSession.

    Returns:
        Spark DataFrame of enriched transactions or None.
    """
    return read_partitioned(spark, enriched_path(cfg), STORED_ENRICHED_SCHEMA)


def read_alerts(cfg: Config, spark: SparkSession) -> DataFrame | None:
    """Reads persisted fraud alerts dataset.

    Args:
        cfg: Application configuration container.
        spark: Active SparkSession.

    Returns:
        Spark DataFrame of alerts or None.
    """
    return read_partitioned(spark, alerts_path(cfg), ALERT_SCHEMA)


def read_profiles(cfg: Config, spark: SparkSession) -> DataFrame | None:
    """Reads persisted customer profile dataset.

    Args:
        cfg: Application configuration container.
        spark: Active SparkSession.

    Returns:
        Spark DataFrame of profiles or None.
    """
    p = Path(profiles_path(cfg))
    if not p.exists():
        return None
    files = [str(f) for f in p.rglob("*.parquet")]
    if not files:
        return None
    return spark.read.parquet(*files)


def read_audit_log(cfg: Config) -> list[dict[str, Any]]:
    """Reads decisions and events from append-only JSONL audit log.

    Args:
        cfg: Application configuration container.

    Returns:
        List of parsed JSON record dictionaries.
    """
    p = Path(audit_path(cfg)) / "decisions.jsonl"
    if not p.exists():
        return []
    lines = p.read_text(encoding="utf-8").strip().splitlines()
    return [json.loads(line) for line in lines if line.strip()]