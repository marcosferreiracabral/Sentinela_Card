"""CLI interface for deterministic transaction audit and replay verification."""

import logging
from typing import Any

from pyspark.sql import SparkSession

from app.audit.replay import RecordNotFound, verify_replay
from app.config import Config

logger = logging.getLogger("sentinela.replay")


def run_replay_cli(spark: SparkSession, cfg: Config, transaction_id: str) -> dict[str, Any]:
    """Executes replay verification for a transaction and prints audit summary.

    Args:
        spark: Active SparkSession.
        cfg: Application configuration container.
        transaction_id: Target transaction identifier.

    Returns:
        Dictionary with comparison verdict and idempotency status.
    """
    print(f"\n=== Audit Replay: Transaction {transaction_id} ===")
    try:
        verdict = verify_replay(spark, cfg, transaction_id)
    except RecordNotFound as exc:
        print(f"Audit record not found: {exc}")
        return {"error": str(exc)}
    except Exception as exc:
        logger.error("replay_execution_failed transaction_id=%s error=%s", transaction_id, exc)
        print(f"Transaction recompute failed: {exc}")
        return {"error": str(exc)}

    stored_lvl = str(verdict.get("stored_level", "UNKNOWN")).upper()
    recomputed_lvl = str(verdict.get("recomputed_level", "UNKNOWN")).upper()
    stored_score = verdict.get("stored_score", 0.0)
    recomputed_score = verdict.get("recomputed_score", 0.0)
    stored_rules = ", ".join(verdict.get("stored_rules", [])) or "None"
    recomputed_rules = ", ".join(verdict.get("recomputed_rules", [])) or "None"

    print(f"Stored Decision      : {stored_lvl} (Score: {stored_score})")
    print(f"Recomputed Decision  : {recomputed_lvl} (Score: {recomputed_score})")
    print(f"Stored Rules         : {stored_rules}")
    print(f"Recomputed Rules     : {recomputed_rules}")

    status_str = "YES (Deterministic)" if verdict.get("idempotent") else "NO (Discrepancy detected)"
    print(f"Idempotency Verified : {status_str}\n")
    return verdict
