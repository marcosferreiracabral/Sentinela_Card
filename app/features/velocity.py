"""Velocity features: count and sum aggregates over temporal sliding windows."""

from math import isfinite


def count_window(counts: list[int], current: int, window_seconds: int) -> int:
    """Counts preceding events falling strictly within relative sliding window [current - window_seconds, current).

    Args:
        counts: List of historical event timestamps in seconds.
        current: Current reference timestamp in seconds.
        window_seconds: Window duration in seconds.

    Returns:
        Integer count of events falling in the window.
    """
    return sum(1 for c in counts if current - window_seconds <= c < current)


def sum_window(amounts: list[float], times: list[int], current: int, window_seconds: int) -> float:
    """Sums transaction amounts occurring strictly within relative sliding window.

    Args:
        amounts: List of transaction amounts.
        times: List of transaction timestamps in seconds.
        current: Current reference timestamp in seconds.
        window_seconds: Window duration in seconds.

    Returns:
        Cumulative transaction sum within the window.
    """
    return sum(a for a, t in zip(amounts, times, strict=False) if current - window_seconds <= t < current)


def is_high_velocity(count: int | None, window_seconds: int, limit: int | None = None) -> bool:
    """Evaluates whether transaction velocity meets or exceeds suspicious threshold.

    Args:
        count: Observed transaction count in window.
        window_seconds: Duration of window in seconds.
        limit: Optional custom trigger count threshold.

    Returns:
        True if observed count meets or exceeds limit.
    """
    if not isfinite(count or 0):
        return False
    if limit is None:
        limit = max(5, 60 // max(1, window_seconds))
    return (count or 0) >= limit


def sliding_windows_are_monotonic(c1: int, c5: int, c60: int, c1440: int) -> bool:
    """Validates temporal monotonicity invariant across hierarchical sliding windows.

    Args:
        c1: 1-minute transaction count.
        c5: 5-minute transaction count.
        c60: 1-hour transaction count.
        c1440: 24-hour transaction count.

    Returns:
        True if c1 <= c5 <= c60 <= c1440.
    """
    if c1440 < c60 or c60 < c5 or c5 < c1:
        return False
    return True
