"""Transaction amount profiling features against 90-day customer historical spending."""

from math import isfinite, sqrt


def mean_std(values: list[float]) -> tuple[float, float]:
    """Calculates sample mean and standard deviation for list of amounts.

    Args:
        values: List of historical transaction amounts.

    Returns:
        Tuple containing sample mean and standard deviation.
    """
    if not values:
        return 0.0, 0.0
    n = len(values)
    mean = sum(values) / n
    if n < 2:
        return mean, 0.0
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return mean, sqrt(var)


def amount_zscore(value: float, mean: float, std: float, min_history_txs: int = 1) -> float:
    """Computes Z-score of transaction amount against customer historical profile.

    Args:
        value: Current transaction amount.
        mean: Historical average transaction amount.
        std: Historical transaction amount standard deviation.
        min_history_txs: Minimum required historical transactions threshold.

    Returns:
        Calculated Z-score value (0.0 if profile variance is insufficient).
    """
    if not isfinite(mean) or mean <= 0:
        return 0.0
    if std is None or not isfinite(std) or std < 0.001:
        return 0.0
    return (value - mean) / std


def amount_vs_avg_ratio(value: float, mean: float) -> float:
    """Calculates ratio between current amount and historical mean.

    Args:
        value: Current transaction amount.
        mean: Historical average transaction amount.

    Returns:
        Ratio multiplier (defaults to 1.0 if mean is invalid).
    """
    if not isfinite(mean) or mean <= 0:
        return 1.0
    return value / mean


def is_unusual_amount(value: float, mean: float, std: float, zscore_min: float = 3.0, ratio_min: float = 1.5) -> bool:
    """Evaluates whether amount deviates significantly from customer spending baseline.

    Args:
        value: Current transaction amount.
        mean: Historical average amount.
        std: Historical standard deviation.
        zscore_min: Minimum Z-score trigger threshold.
        ratio_min: Minimum mean multiplier trigger threshold.

    Returns:
        True if either Z-score or ratio exceeds configured thresholds.
    """
    z = amount_zscore(value, mean, std)
    r = amount_vs_avg_ratio(value, mean)
    return z >= zscore_min or r >= ratio_min
