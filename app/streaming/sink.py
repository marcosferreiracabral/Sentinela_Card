"""Pipeline sinks: real-time streaming decision console formatter and statistics."""

import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame

from app.config import Config

COLORS: dict[str, str] = {"APPROVE": "\033[32m", "REVIEW": "\033[33m", "BLOCK": "\033[31m"}
RESET: str = "\033[0m"

_MERCHANT_LABELS: dict[str, str] = {
    "5411": "Supermarket",
    "5812": "Restaurant",
    "5814": "Fast Food",
}


def _brl(v: float) -> str:
    """Formats monetary amount to Brazilian Real (BRL) currency string.

    Args:
        v: Numeric monetary value.

    Returns:
        Formatted BRL string.
    """
    s = f"{v:,.2f}"
    return "R$ " + s.replace(",", "@").replace(".", ",").replace("@", ".")


def load_merchant_names(cfg: Config) -> dict[str, str]:
    """Loads merchant name lookup mapping from metadata storage.

    Args:
        cfg: Application configuration container.

    Returns:
        Dictionary mapping merchant IDs to commercial store names.
    """
    p = Path(cfg.data_dir) / "metadata" / "merchants.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def merchant_label(merchant_id: str, merchant_category: str | None, names: dict[str, str]) -> str:
    """Resolves merchant display name from ID, category mapping, or fallback string.

    Args:
        merchant_id: Unique merchant identifier.
        merchant_category: Merchant Category Code (MCC).
        names: Dictionary of resolved merchant names.

    Returns:
        Resolved merchant display string.
    """
    if merchant_id in names:
        return names[merchant_id]
    mcc = merchant_category or ""
    return _MERCHANT_LABELS.get(mcc, merchant_id)


def print_decisions(enriched: DataFrame, cfg: Config, names: dict[str, str] | None = None) -> None:
    """Prints streaming decision log lines to standard output.

    Args:
        enriched: Enriched transactions DataFrame.
        cfg: Application configuration container.
        names: Optional preloaded merchant names dictionary.
    """
    if enriched.count() == 0:
        return
    names = names or load_merchant_names(cfg)
    rows = enriched.toPandas()
    for _, r in rows.iterrows():
        level = str(r["risk_level"]).upper()
        label = merchant_label(str(r["merchant_id"]), r.get("merchant_category"), names)
        score = int(round(float(r["risk_score"])))
        rules = list(r.get("triggered_rules") or [])
        suffix = f" ({', '.join(rules)})" if rules else ""
        color = COLORS.get(level, "")
        print(
            f"{color}[{level:8s}]{RESET} {r['transaction_id']} {_brl(float(r['amount']))} {label} - score {score}{suffix}",
            flush=True,
        )


def print_stats(stats: dict[str, Any]) -> None:
    """Prints aggregated session statistics to standard output.

    Args:
        stats: Dictionary containing execution count metrics and triggered rules.
    """
    print("\n=== Session Statistics ===")
    print(f"Processed        : {stats.get('processed', 0)}")
    print(f"Approved         : {stats.get('approve', 0)}")
    print(f"Review           : {stats.get('review', 0)}")
    print(f"Blocked          : {stats.get('block', 0)}")
    print(f"Alerts           : {stats.get('alerts', 0)}")
    print(f"Average score    : {stats.get('avg_score', 0)}")
    triggered = stats.get("triggered_by_rule") or {}
    if triggered:
        print("Triggered rules  :")
        for rule, n in sorted(triggered.items(), key=lambda kv: -kv[1]):
            print(f"  - {rule}: {n}x")
