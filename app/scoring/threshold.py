"""Decision threshold mapping for fraud score buckets."""

from app.config import Thresholds


def decide(score: float, thresholds: Thresholds | None = None) -> str:
    """Classifies transaction score into decision category ('approve', 'review', 'block').

    Args:
        score: Risk score value between 0.0 and 100.0.
        thresholds: Optional custom Thresholds instance (defaults to default thresholds).

    Returns:
        Decision action string.
    """
    if thresholds is None:
        thresholds = Thresholds()
    return thresholds.decision(score)
