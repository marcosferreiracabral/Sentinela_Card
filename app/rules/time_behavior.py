"""Unusual hour and location detection rule: flags overnight purchases in unfamiliar cities."""

from typing import Any

from app.rules.engine import FraudRule, RuleResult


class UnusualHourAndPlaceRule(FraudRule):
    """Detects suspicious transactions conducted during late-night hours in previously unvisited cities."""

    name: str = "unusual_hour_and_place"

    def evaluate(self, tx: dict[str, Any], features: dict[str, Any]) -> RuleResult:
        """Evaluates whether transaction combines late-night hour with location novelty.

        Args:
            tx: Raw transaction attributes.
            features: Precalculated temporal and geographic novelty features.

        Returns:
            RuleResult with evaluation verdict and reason.
        """
        start = int(self.params.get("untrusted_hour_start", 2))
        end = int(self.params.get("untrusted_hour_end", 4))
        min_amount = float(self.params.get("min_amount", 150.0))
        hour = features.get("hour_of_day")
        new_city = bool(features.get("new_city"))
        amount = float(tx.get("amount") or 0)

        if hour is None or amount < min_amount:
            return self.noop()
        prev_city = features.get("prev_merchant_city")
        is_late_hour = start <= int(hour) <= end
        if is_late_hour and new_city and prev_city and prev_city != tx.get("merchant_city"):
            return self.result(
                f"transação na madrugada ({int(hour)}h) em cidade não habitual ({tx.get('merchant_city')})"
            )
        return self.noop()
