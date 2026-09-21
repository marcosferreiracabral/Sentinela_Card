"""New device high-amount detection rule: flags large purchases on newly observed hardware or browser identifiers."""

from typing import Any

from app.rules.engine import FraudRule, RuleResult


class NewDeviceHighAmountRule(FraudRule):
    """Detects account takeover or unauthorized checkout from unfamiliar devices with high monetary value."""

    name: str = "new_device_high_amount"

    def evaluate(self, tx: dict[str, Any], features: dict[str, Any]) -> RuleResult:
        """Evaluates whether device novelty combined with spending spike exceeds threshold.

        Args:
            tx: Raw transaction attributes.
            features: Precalculated device state and spending baseline features.

        Returns:
            RuleResult with evaluation verdict and reason.
        """
        amount_ratio = float(self.params.get("amount_ratio", 4.0))
        min_amount = float(self.params.get("min_amount", 800.0))
        new_device = bool(features.get("new_device"))
        amount = float(tx.get("amount") or 0)
        mean = float(features.get("amount_mean_90d") or 0)

        if not new_device:
            return self.noop()
        if not tx.get("device_id"):
            return self.noop()
        if amount <= min_amount:
            return self.noop()
        if mean > 0 and amount > amount_ratio * mean:
            ratio = amount / mean
            return self.result(
                f"dispositivo nunca visto ({tx.get('device_id')}) com valor R$ {amount:.2f} ({ratio:.1f}x a média R$ {mean:.2f})"
            )
        return self.noop()