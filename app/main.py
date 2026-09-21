"""Sentinela_Card real-time credit card fraud detection CLI.

Available subcommands:
  - generate: Generate synthetic transaction datasets with ground-truth fraud patterns.
  - backfill: Execute PySpark batch processing over historical Parquet files.
  - stream: Run local socket streaming simulation and evaluation.
  - analyze: Execute analytical Spark SQL queries against persisted data layers.
  - replay: Recompute and verify deterministic audit trails for specific transactions.
  - tune: Adjust decision thresholds in rules.yaml.
  - export: Export processed tables to Parquet, CSV, or JSONL formats.
"""

import argparse
import logging
import sys
from pathlib import Path

_ROOT = str(Path(__file__).resolve().parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app import _setup_windows_java

_setup_windows_java()

from pyspark.sql import SparkSession

from app.analytics.engine import run_analysis
from app.analytics.queries import QUERIES
from app.cli.backfill import run_backfill
from app.cli.replay import run_replay_cli
from app.cli.simulate import run_stream_demo
from app.config import load_config, save_thresholds
from data.generator import generate

logger = logging.getLogger("sentinela.cli")


def get_spark_session(app_name: str = "Sentinela_Card") -> SparkSession:
    """Initializes or retrieves local SparkSession configured for pipeline execution.

    Args:
        app_name: Application identifier for SparkSession.

    Returns:
        Configured SparkSession instance.
    """
    _setup_windows_java()
    logger.info("Initializing SparkSession app_name=%s", app_name)

    session = (
        SparkSession.builder.appName(app_name)
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def cmd_generate(args: argparse.Namespace) -> None:
    """Handles synthetic data generation command.

    Args:
        args: Parsed command-line arguments.
    """
    output_dir = args.output
    n_tx = args.transactions
    print(f"Generating {n_tx} synthetic transactions into {output_dir}")
    _, summary = generate(
        spark=None,
        n_transactions=n_tx,
        output_dir=output_dir,
        write=True,
    )
    print("Dataset generation completed:")
    for k, v in summary.items():
        print(f"  - {k}: {v}")


def cmd_backfill(args: argparse.Namespace) -> None:
    """Handles batch backfill processing command.

    Args:
        args: Parsed command-line arguments.
    """
    spark = get_spark_session("Sentinela_Backfill")
    cfg = load_config(rules_path=args.rules, data_dir=args.data_dir)
    source_p = Path(args.source).resolve()
    if source_p.is_dir():
        seed_p = source_p / "seed_transactions.parquet"
        if seed_p.exists():
            source_p = seed_p
    run_backfill(spark, cfg, str(source_p), limit=args.limit)


def cmd_stream(args: argparse.Namespace) -> None:
    """Handles real-time streaming simulation command.

    Args:
        args: Parsed command-line arguments.
    """
    spark = get_spark_session("Sentinela_Stream")
    cfg = load_config(rules_path=args.rules, data_dir=args.data_dir)
    source_p = Path(args.source).resolve()
    if source_p.is_dir():
        seed_p = source_p / "seed_transactions.parquet"
        if seed_p.exists():
            source_p = seed_p
    run_stream_demo(spark, cfg, source=str(source_p), speed=args.speed, limit=args.limit)


def cmd_analyze(args: argparse.Namespace) -> None:
    """Handles analytical query execution command.

    Args:
        args: Parsed command-line arguments.
    """
    spark = get_spark_session("Sentinela_Analyze")
    cfg = load_config(rules_path=args.rules, data_dir=args.data_dir)
    query_name = args.query
    if query_name not in QUERIES and not query_name.upper().startswith("SELECT"):
        print(f"Unknown query: '{query_name}'. Available predefined queries:")
        for q in QUERIES:
            print(f"  - {q}")
        sys.exit(1)
    run_analysis(spark, cfg, query_name, show=True)


def cmd_replay(args: argparse.Namespace) -> None:
    """Handles deterministic transaction replay command.

    Args:
        args: Parsed command-line arguments.
    """
    spark = get_spark_session("Sentinela_Replay")
    cfg = load_config(rules_path=args.rules, data_dir=args.data_dir)
    run_replay_cli(spark, cfg, args.transaction_id)


def cmd_tune(args: argparse.Namespace) -> None:
    """Handles threshold update command.

    Args:
        args: Parsed command-line arguments.
    """
    rules_path = args.rules
    review = args.threshold_review
    block = args.threshold_block
    save_thresholds(rules_path, review=review, block=block)
    print(f"Configuration file '{rules_path}' updated:")
    print(f"  - review_from: {review}")
    print(f"  - block_from: {block}")


def cmd_export(args: argparse.Namespace) -> None:
    """Handles data layer export command.

    Args:
        args: Parsed command-line arguments.
    """
    spark = get_spark_session("Sentinela_Export")
    cfg = load_config(rules_path=args.rules, data_dir=args.data_dir)
    fmt = args.format.lower()
    target = args.target.lower()
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    from app.storage.reader import read_alerts, read_enriched, read_raw

    readers = {"alerts": read_alerts, "enriched": read_enriched, "raw": read_raw}
    reader_fn = readers.get(target, read_alerts)
    df = reader_fn(cfg, spark)
    if df is None or df.count() == 0:
        print(f"No records found for target layer '{target}'")
        return

    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    pdf = df.toPandas()
    if "timestamp" in pdf.columns:
        pdf["timestamp"] = pd.to_datetime(pdf["timestamp"]).astype("datetime64[us]")
    if "decision_timestamp" in pdf.columns:
        pdf["decision_timestamp"] = pd.to_datetime(pdf["decision_timestamp"]).astype("datetime64[us]")

    if fmt == "parquet":
        out_path = out_dir / f"{target}_export.parquet"
        pq.write_table(pa.Table.from_pandas(pdf), str(out_path))
    elif fmt == "csv":
        out_path = out_dir / f"{target}_export.csv"
        pdf.to_csv(str(out_path), index=False, encoding="utf-8")
    elif fmt in ("json", "jsonl"):
        out_path = out_dir / f"{target}_export.jsonl"
        pdf.to_json(str(out_path), orient="records", lines=True, force_ascii=False)
    else:
        print(f"Unsupported format '{fmt}' (supported: parquet, csv, json)")
        return
    print(f"Export completed: {out_path} ({fmt})")


def build_parser() -> argparse.ArgumentParser:
    """Builds CLI argument parser with subcommands.

    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        prog="Sentinela_Card",
        description="Real-Time Credit Card Fraud Detection System (PySpark + SQL)",
    )
    parser.add_argument("--rules", default="rules.yaml", help="Path to rules.yaml file")
    parser.add_argument("--data-dir", default="data", help="Base data directory")

    subparsers = parser.add_subparsers(dest="subcommand", help="Subcommand to execute")

    p_gen = subparsers.add_parser("generate", help="Generate synthetic transactions")
    p_gen.add_argument("--transactions", type=int, default=100000, help="Number of transactions to generate")
    p_gen.add_argument("--output", default="data", help="Output directory path")

    p_bf = subparsers.add_parser("backfill", help="Process historical Parquet batch")
    p_bf.add_argument("--source", default="data/seed_transactions.parquet", help="Source Parquet path")
    p_bf.add_argument("--limit", type=int, default=0, help="Transaction limit (0 = process all)")

    p_str = subparsers.add_parser("stream", help="Run real-time streaming simulation via socket")
    p_str.add_argument("--source", default="data/seed_transactions.parquet", help="Transaction source path")
    p_str.add_argument("--speed", default="realtime", choices=["realtime", "fast", "burst"], help="Simulation speed")
    p_str.add_argument("--limit", type=int, default=0, help="Transaction count limit")

    p_ana = subparsers.add_parser("analyze", help="Execute analytical Spark SQL queries")
    p_ana.add_argument(
        "--query",
        default="fraud_rate_by_category",
        help="Predefined query name or raw SQL string",
    )

    p_rep = subparsers.add_parser("replay", help="Audit and recompute transaction decision")
    p_rep.add_argument("--transaction-id", required=True, help="Target transaction ID")

    p_tune = subparsers.add_parser("tune", help="Update review and block thresholds")
    p_tune.add_argument("--threshold-review", type=int, required=True, help="New review threshold")
    p_tune.add_argument("--threshold-block", type=int, required=True, help="New block threshold")

    p_exp = subparsers.add_parser("export", help="Export processed data layers")
    p_exp.add_argument("--target", default="alerts", choices=["alerts", "enriched", "raw"], help="Target layer")
    p_exp.add_argument("--format", default="parquet", choices=["parquet", "csv", "json"], help="Export format")
    p_exp.add_argument("--output", default="alerts", help="Output directory path")

    return parser


def main(args: list[str] | None = None) -> None:
    """Main CLI execution entrypoint.

    Args:
        args: Optional list of command-line argument strings.
    """
    parser = build_parser()
    parsed = parser.parse_args(args)

    if not parsed.subcommand:
        parser.print_help()
        sys.exit(0)

    handlers = {
        "generate": cmd_generate,
        "backfill": cmd_backfill,
        "stream": cmd_stream,
        "analyze": cmd_analyze,
        "replay": cmd_replay,
        "tune": cmd_tune,
        "export": cmd_export,
    }

    handler = handlers.get(parsed.subcommand)
    if handler:
        handler(parsed)


if __name__ == "__main__":
    main()
