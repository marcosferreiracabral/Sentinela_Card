"""Unusual amount detection rule: flags spending deviations exceeding customer 90-day statistical baseline."""

from typing import Any

from app.rules.engine import FraudRule, RuleResult


class UnusualAmountRule(FraudRule):
    """Detects statistically significant spending deviations against customer historical average."""

    name: str = "unusual_amount"

    def evaluate(self, tx: dict[str, Any], features: dict[str, Any]) -> RuleResult:
        """Evaluates Z-score and ratio metrics against customer 90-day amount profile.

        Args:
            tx: Raw transaction attributes.
            features: Precalculated statistical profile features.

        Returns:
            RuleResult with evaluation verdict and reason.
        """
        zscore_min = float(self.params.get("zscore_min", 5.5))
        ratio_min = float(self.params.get("ratio_min", 4.0))
        min_amount = float(self.params.get("min_amount", 800.0))
        min_txs = int(self.params.get("min_history_txs", 5))

        zscore = features.get("amount_zscore") or 0.0
        ratio = features.get("amount_vs_avg_ratio") or 1.0
        hist_count = int(features.get("amount_count_90d") or 0)
        amount = float(tx.get("amount") or 0)

        if hist_count < min_txs or amount < min_amount:
            return self.noop()
        if zscore >= zscore_min and ratio >= ratio_min:
            mean = features.get("amount_mean_90d", 0)
            return self.result(f"valor R$ {amount:.2f} fora do padrão histórico (z={zscore:.1f}, média R$ {mean:.2f})")
        return self.noop()
