"""Historical batch processing and backfill orchestration."""

import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from app.config import Config
from app.streaming.processor import FraudPipeline
from app.streaming.sink import print_stats


def run_backfill(spark: SparkSession, cfg: Config, source_path: str | Path, limit: int = 0) -> dict[str, Any]:
    """Executes fraud pipeline in batch mode over historical Parquet dataset.

    Args:
        spark: Active SparkSession.
        cfg: Application configuration container.
        source_path: Path to input Parquet files.
        limit: Optional maximum row count to process (0 = all records).

    Returns:
        Dictionary containing batch execution metrics and decision statistics.
    """
    source_str = str(source_path)
    print(f"Starting batch backfill from source: {source_str}")
    start_time = time.time()

    df = spark.read.parquet(source_str)
    if limit and limit > 0:
        df = df.limit(limit)

    total_rows = df.count()
    print(f"Loaded {total_rows} input transactions. Processing with PySpark engine")

    pipeline = FraudPipeline(spark, cfg, write=True)
    result = pipeline.process_batch(df)

    elapsed = time.time() - start_time
    rate = total_rows / max(0.01, elapsed)
    print(f"\nBatch processing completed in {elapsed:.2f}s ({rate:.0f} tx/s)")
    print_stats(result.stats)
    return result.stats
