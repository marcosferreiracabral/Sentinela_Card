"""Merchant risk profiling and high-risk MCC (Merchant Category Code) evaluation."""

HIGH_RISK_MCCS: set[str] = {"7995", "7996", "6051", "4829", "5816", "5999", "7800", "6012"}


def is_high_risk_merchant(merchant_category: str | None, high_risk: set[str] | None = None) -> bool:
    """Evaluates whether merchant category represents elevated fraud exposure (gambling, crypto, remittance).

    Args:
        merchant_category: Four-digit MCC string.
        high_risk: Optional custom set of risky MCC strings (defaults to HIGH_RISK_MCCS).

    Returns:
        True if merchant_category matches configured high-risk list.
    """
    if not merchant_category:
        return False
    mcc = merchant_category.strip()
    return mcc in (high_risk or HIGH_RISK_MCCS)