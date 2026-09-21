"""BIN attack detection rule: flags consecutive authorization denials across same BIN prefix."""

from typing import Any

from app.rules.engine import FraudRule, RuleResult


class BinAttackRule(FraudRule):
    """Detects brute-force card enumeration attacks through consecutive declines on the same BIN."""

    name: str = "bin_attack"

    def evaluate(self, tx: dict[str, Any], features: dict[str, Any]) -> RuleResult:
        """Evaluates BIN denial velocity within 15-minute window.

        Args:
            tx: Raw transaction attributes.
            features: Precalculated temporal and BIN window features.

        Returns:
            RuleResult indicating whether BIN attack signature matched.
        """
        denials = int(features.get("bin_denials_15min") or 0)
        min_denials = int(self.params.get("min_denials", 3))
        was_approved = bool(features.get("prev_bin_approved"))

        if denials >= min_denials and not was_approved:
            bin_id = str(tx.get("card_id") or "")[:6]
            return self.result(f"{denials} negadas em {bin_id} nos últimos 15min sem aprovação prévia")
        return self.noop()