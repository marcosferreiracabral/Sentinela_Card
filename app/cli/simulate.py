"""Real-time streaming simulation: socket transaction injection and PySpark Structured Streaming evaluation."""

import json
import socket
import threading
import time
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from app.config import Config
from app.streaming.processor import FraudPipeline
from app.streaming.sink import load_merchant_names, print_decisions, print_stats
from app.streaming.source import read_socket

SPEED_REALTIME = "realtime"
SPEED_FAST = "fast"
SPEED_BURST = "burst"


class SocketProducer(threading.Thread):
    """Local TCP server streaming JSON formatted transaction lines to socket consumer."""

    def __init__(self, host: str, port: int, lines: list[str], delays: list[float]) -> None:
        """Initializes TCP socket server thread.

        Args:
            host: Bind hostname or IP.
            port: Bind TCP port.
            lines: List of JSON-encoded transaction records.
            delays: Injection delay seconds corresponding to each line.
        """
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.lines = lines
        self.delays = delays
        self._stop = threading.Event()

    def stop(self) -> None:
        """Signals background server thread to stop."""
        self._stop.set()

    def run(self) -> None:
        """Accepts incoming TCP connection and streams transaction lines with configured delays."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            reuse_addr = getattr(socket, "SO_REUSEADDR", 1)
            server.setsockopt(socket.SOL_SOCKET, reuse_addr, 1)
            server.bind((self.host, self.port))
            server.listen(1)
            server.settimeout(0.5)
            while not self._stop.is_set():
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    continue
                with conn:
                    for i, line in enumerate(self.lines):
                        if self._stop.is_set():
                            break
                        conn.sendall((line + "\n").encode("utf-8"))
                        delay = self.delays[i]
                        if delay > 0:
                            time.sleep(delay)
                    conn.shutdown(socket.SHUT_WR)
                self._stop.set()


def compute_delays(rows: list[dict[str, Any]], speed: str) -> list[float]:
    """Calculates sleep intervals between consecutive transactions based on playback speed mode.

    Args:
        rows: List of transaction dictionaries containing timestamp.
        speed: Speed profile name ('realtime', 'fast', 'burst').

    Returns:
        List of delay values in seconds.
    """
    delays: list[float] = []
    prev_ts: datetime | None = None
    for r in rows:
        ts = r["timestamp"]
        if not isinstance(ts, datetime):
            ts = datetime.fromisoformat(str(ts))
        if speed == SPEED_BURST:
            delays.append(0.0)
        elif speed == SPEED_FAST:
            delays.append(0.01)
        else:
            if prev_ts is None:
                delays.append(0.0)
            else:
                dt = max(0.0, (ts - prev_ts).total_seconds())
                delays.append(max(0.02, min(1.5, dt)))
        prev_ts = ts
    return delays


def load_seed_lines(spark: SparkSession | None, source: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Loads seed Parquet dataset and serializes records into JSON strings.

    Args:
        spark: Active SparkSession or None.
        source: File path to seed Parquet dataset.

    Returns:
        Tuple containing list of raw record dictionaries and serialized JSON lines.
    """
    import pyarrow.parquet as pq

    table = pq.read_table(source)
    rows = table.to_pandas().sort_values(["timestamp", "transaction_id"])
    records: list[dict[str, Any]] = []
    for _, r in rows.iterrows():
        record: dict[str, Any] = {}
        for col in (
            "transaction_id",
            "customer_id",
            "card_id",
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
        ):
            record[col] = r[col]
        record["timestamp"] = (
            r["timestamp"].isoformat() if hasattr(r["timestamp"], "isoformat") else str(r["timestamp"])
        )
        record["amount"] = round(float(r["amount"]), 2)
        records.append(record)
    lines = [json.dumps(x, ensure_ascii=False) for x in records]
    return records, lines


def run_stream_demo(
    spark: SparkSession, cfg: Config, source: str, speed: str = SPEED_FAST, limit: int = 0
) -> dict[str, Any]:
    """Orchestrates real-time streaming demo with local socket source and Structured Streaming.

    Args:
        spark: Active SparkSession.
        cfg: Application configuration container.
        source: File path to source transactions Parquet.
        speed: Simulation speed profile ('realtime', 'fast', 'burst').
        limit: Optional transaction count limit.

    Returns:
        Dictionary of aggregated session statistics.
    """
    from app.streaming.sink import COLORS, RESET

    records, lines = load_seed_lines(spark, source)
    if limit and limit > 0:
        records, lines = records[:limit], lines[:limit]
    delays = compute_delays(records, speed)

    producer = SocketProducer(cfg.streaming.socket_host, cfg.streaming.socket_port, lines, delays)
    producer.start()
    time.sleep(0.5)

    aggregated: dict[str, Any] = {
        "processed": 0,
        "approve": 0,
        "review": 0,
        "block": 0,
        "alerts": 0,
        "triggered_by_rule": {},
    }
    rules_counts: dict[str, int] = {}
    lock = threading.Lock()
    names = load_merchant_names(cfg)

    print(f"{COLORS['BLOCK']}Sentinela_Card real-time streaming simulation{RESET}")
    print(f"Source: {source} | Speed: {speed} | Transactions: {len(lines)}\n")

    def batch_fn(batch_df: DataFrame, epoch_id: int) -> None:
        if batch_df.count() == 0:
            return
        pipeline = FraudPipeline(spark, cfg)
        result = pipeline.process_batch(batch_df)
        print_decisions(result.enriched, cfg, names=names)
        stats: dict[str, Any] = result.stats
        with lock:
            aggregated["processed"] = int(aggregated.get("processed", 0)) + int(stats.get("processed", 0))
            aggregated["approve"] = int(aggregated.get("approve", 0)) + int(stats.get("approve", 0))
            aggregated["review"] = int(aggregated.get("review", 0)) + int(stats.get("review", 0))
            aggregated["block"] = int(aggregated.get("block", 0)) + int(stats.get("block", 0))
            aggregated["alerts"] = int(aggregated.get("alerts", 0)) + int(stats.get("alerts", 0))
            for rule, n in (stats.get("triggered_by_rule") or {}).items():
                rules_counts[rule] = rules_counts.get(rule, 0) + int(n)
            aggregated["triggered_by_rule"] = dict(rules_counts)

    query = (
        read_socket(spark, cfg)
        .writeStream.foreachBatch(batch_fn)
        .option("checkpointLocation", cfg.streaming.checkpoint_dir)
        .trigger(processingTime=f"{cfg.streaming.processing_time_seconds} seconds")
        .start()
    )

    try:
        query.awaitTermination()
    finally:
        producer.stop()
    print_stats(aggregated)
    return aggregated
