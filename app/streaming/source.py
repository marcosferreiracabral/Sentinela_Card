"""Streaming sources for socket demo and production Kafka ingestion."""

import json
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.config import Config
from app.schemas.transaction import TRANSACTION_SCHEMA


def read_socket(spark: SparkSession, cfg: Config) -> DataFrame:
    """Reads transactions from TCP socket for local demonstrations.

    Args:
        spark: Active SparkSession.
        cfg: Application configuration container.

    Returns:
        Streaming DataFrame with parsed transaction schema and watermark applied.
    """
    host = cfg.streaming.socket_host
    port = cfg.streaming.socket_port
    lines = (
        spark.readStream.format("socket")
        .option("host", host)
        .option("port", port)
        .option("includeTimestamp", "false")
        .load()
    )
    parsed = lines.select(F.from_json(F.col("value"), TRANSACTION_SCHEMA).alias("tx")).select("tx.*")
    return parsed.withWatermark("timestamp", f"{cfg.streaming.watermark_seconds} seconds")


def read_kafka(spark: SparkSession, cfg: Config, bootstrap_servers: str, topic: str) -> DataFrame:
    """Reads transactions from Apache Kafka topic for production deployments.

    Args:
        spark: Active SparkSession.
        cfg: Application configuration container.
        bootstrap_servers: Comma-separated list of Kafka broker host:port endpoints.
        topic: Kafka topic name to subscribe.

    Returns:
        Streaming DataFrame with parsed transaction schema and watermark applied.
    """
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    value = df.selectExpr("CAST(value AS STRING) AS raw_value")
    parsed = value.select(F.from_json(F.col("raw_value"), TRANSACTION_SCHEMA).alias("tx")).select("tx.*")
    return parsed.withWatermark("timestamp", f"{cfg.streaming.watermark_seconds} seconds")


def json_line(row: Any) -> str:
    """Serializes a raw transaction record into a JSON string line.

    Args:
        row: Transaction record (dict, namedtuple, or pandas Series/Row).

    Returns:
        Serialized JSON string line.
    """
    payload = {}
    for field in TRANSACTION_SCHEMA.fieldNames():
        v = row[field]
        if hasattr(v, "item"):
            v = v.item()
        if v is not None and hasattr(v, "isoformat"):
            v = v.isoformat()
        payload[field] = v
    return json.dumps(payload, ensure_ascii=False)
