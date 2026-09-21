"""Score aggregation and rule weight combination engine."""

from app.rules.engine import RuleResult


def combine(results: list[RuleResult], max_score: float = 100.0) -> tuple[float, list[str], list[str]]:
    """Combines triggered rule weights into aggregate risk score bounded by max_score.

    Args:
        results: Evaluated RuleResult instances for a transaction.
        max_score: Upper ceiling for composite score (default: 100.0).

    Returns:
        Tuple containing composite score, list of triggered rule names, and list of reason strings.
    """
    triggered = [r for r in results if r.triggered]
    score = sum(r.weight for r in triggered)
    score = max(0.0, min(float(max_score), float(score)))
    rules = [r.rule_name for r in triggered]
    reasons = [r.reason for r in triggered if r.reason]
    return score, rules, reasons