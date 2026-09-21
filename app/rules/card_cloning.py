"""Card cloning detection rule: low-value probe transaction followed by high-value purchase, or magnetic stripe fallback on chip-only history."""

from typing import Any

from app.rules.engine import FraudRule, RuleResult


class CardCloningRule(FraudRule):
    """Detects card cloning patterns: low-value probe transaction followed by high-value spike or magnetic stripe on chip-only card."""

    name: str = "card_cloning"

    def evaluate(self, tx: dict[str, Any], features: dict[str, Any]) -> RuleResult:
        """Evaluates transaction sequence for probe test or magnetic stripe downgrade signatures.

        Args:
            tx: Raw transaction attributes.
            features: Precalculated temporal sliding windows and state features.

        Returns:
            RuleResult with evaluation verdict and reason.
        """
        amount = float(tx.get("amount") or 0)
        test_max = float(self.params.get("test_amount_max", 10.0))
        follow_min = float(self.params.get("follow_window_min", 10))
        follow_ratio = float(self.params.get("follow_ratio", 5.0))

        min_10 = features.get("min_10min") or 0
        count_10 = int(features.get("count_10min") or 0)
        minutes_since_last = features.get("minutes_since_last")
        new_terminal = bool(features.get("new_terminal"))
        same_city = features.get("prev_merchant_city") == tx.get("merchant_city")
        city_changed = features.get("prev_merchant_city") in (None, "") or not same_city
        chip_only = bool(features.get("entry_mode_chip_only_history"))
        magnetic_now = tx.get("entry_mode") == "magnetic"

        test_then_big = (
            count_10 >= 1
            and 0 < float(min_10) < test_max
            and amount >= follow_ratio * float(min_10)
            and minutes_since_last is not None
            and 0 < minutes_since_last <= follow_min
        )
        stripe_on_chip_card = magnetic_now and chip_only and amount > 0
        same_city_new_terminal = same_city and (new_terminal or not city_changed) and amount > 0

        if test_then_big and (same_city_new_terminal or stripe_on_chip_card):
            reason = (
                f"teste R$ {float(min_10):.2f} seguido de R$ {amount:.2f} "
                f"em {minutes_since_last:.0f}min ({count_10} tx em 10min)"
            )
            return self.result(reason)
        if stripe_on_chip_card and amount >= 3 * test_max:
            return self.result(f"tarja magnética em cartão com histórico 100% chip (R$ {amount:.2f})")
        return self.noop()