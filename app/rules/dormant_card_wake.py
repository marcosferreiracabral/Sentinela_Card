"""Dormant card reactivation rule: flags sudden high-value spend following prolonged inactivity."""

from typing import Any

from app.rules.engine import FraudRule, RuleResult


class DormantCardWakeRule(FraudRule):
    """Detects sudden high-value spending on long-dormant credit cards."""

    name: str = "dormant_card_wake"

    def evaluate(self, tx: dict[str, Any], features: dict[str, Any]) -> RuleResult:
        """Evaluates dormancy duration and spend spike magnitude against historical baseline.

        Args:
            tx: Raw transaction attributes.
            features: Precalculated temporal and spending baseline features.

        Returns:
            RuleResult with evaluation verdict and reason.
        """
        dormant_limit = float(self.params.get("dormant_days", 180))
        amount_ratio = float(self.params.get("amount_ratio", 2.0))
        min_amount = float(self.params.get("min_amount", 500.0))

        dormant = features.get("dormant_days") or 0.0
        amount = float(tx.get("amount") or 0)
        mean = float(features.get("amount_mean_90d") or 0)

        if dormant <= dormant_limit:
            return self.noop()
        if amount <= min_amount:
            return self.noop()
        if mean > 0 and amount > amount_ratio * mean:
            ratio = amount / mean
            return self.result(
                f"cartão inativo {int(dormant)} dias: transação de R$ {amount:.2f} ({ratio:.1f}x média R$ {mean:.2f})"
            )
        if mean <= 0 and amount >= 2 * min_amount:
            return self.result(f"cartão inativo {int(dormant)} dias: reativação com valor R$ {amount:.2f}")
        return self.noop()
