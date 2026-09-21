"""Time-based behavioral features (unusual operating hours and card dormancy)."""

from datetime import datetime


def is_untrusted_hour(hour: int, start: int = 2, end: int = 5) -> bool:
    """Evaluates whether transaction occurs during high-risk overnight hours.

    Args:
        hour: Hour of the day in UTC/local timezone (0-23).
        start: Start hour of untrusted interval inclusive (default: 2).
        end: End hour of untrusted interval inclusive (default: 5).

    Returns:
        True if hour falls within [start, end].
    """
    return start <= hour <= end


def dormant_days_since(last_active_dt: datetime | None, current_dt: datetime | None) -> float:
    """Computes elapsed days since preceding card transaction.

    Args:
        last_active_dt: Datetime of previous active transaction.
        current_dt: Datetime of current transaction.

    Returns:
        Elapsed days as float (0.0 if either timestamp is missing).
    """
    if last_active_dt is None or current_dt is None:
        return 0.0
    delta = current_dt - last_active_dt
    return delta.total_seconds() / 86400.0


def is_dormant_card(dormant_days: float | None, limit: float = 180.0) -> bool:
    """Evaluates whether card has been inactive beyond dormancy threshold.

    Args:
        dormant_days: Elapsed days since prior transaction.
        limit: Threshold limit in days (default: 180.0).

    Returns:
        True if dormant_days strictly exceeds limit.
    """
    return dormant_days is not None and dormant_days > limit