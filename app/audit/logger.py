"""Audit trail logging for fraud decisions and alert generation."""

from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame

from app.config import Config
from app.storage.writer import append_audit


def utc_now() -> str:
    """Returns current UTC timestamp in ISO-8601 format.

    Returns:
        ISO-8601 formatted UTC timestamp string.
    """
    return datetime.now(timezone.utc).isoformat()


def audit_rows_from_enriched(enriched: DataFrame, cfg: Config) -> list[dict[str, Any]]:
    """Transforms enriched Spark DataFrame rows into structured audit decision dictionaries.

    Args:
        enriched: Spark DataFrame containing processed and scored transactions.
        cfg: Application configuration container.

    Returns:
        List of audit record dictionaries.
    """
    rows = enriched.toPandas()
    out: list[dict[str, Any]] = []
    for _, r in rows.iterrows():
        tr = r.get("triggered_rules")
        triggered = list(tr) if tr is not None and hasattr(tr, "__len__") and len(tr) > 0 else []
        rr = r.get("rule_reasons")
        reasons = list(rr) if rr is not None and hasattr(rr, "__len__") and len(rr) > 0 else []
        out.append(
            {
                "event": "decision",
                "transaction_id": r["transaction_id"],
                "customer_id": r["customer_id"],
                "card_id": r["card_id"],
                "timestamp": str(r["timestamp"]),
                "amount": float(r["amount"] or 0),
                "risk_score": round(float(r.get("risk_score") or 0), 2),
                "risk_level": r.get("risk_level"),
                "triggered_rules": triggered,
                "rule_reasons": reasons,
                "model_version": r.get("model_version"),
                "audit_ts": utc_now(),
            }
        )
    return out


def log_decisions(enriched: DataFrame, cfg: Config) -> None:
    """Persists decision records to JSONL audit log.

    Args:
        enriched: Enriched transactions DataFrame.
        cfg: Application configuration container.
    """
    rows = audit_rows_from_enriched(enriched, cfg)
    if rows:
        append_audit(rows, cfg)


def log_alert(detail: dict[str, Any], cfg: Config) -> None:
    """Appends single alert event to audit log.

    Args:
        detail: Alert payload dictionary.
        cfg: Application configuration container.
    """
    row = {"event": "alert", **detail, "audit_ts": utc_now()}
    append_audit([row], cfg)
