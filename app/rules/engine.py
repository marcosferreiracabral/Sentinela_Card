"""Independent fraud rule evaluation engine and rule registry."""

import logging
from dataclasses import dataclass
from typing import Any

from app.config import Config, RuleCfg

logger = logging.getLogger("sentinela.rules")


@dataclass
class RuleResult:
    """Outcome container of an evaluated fraud detection rule."""

    rule_name: str
    triggered: bool = False
    reason: str = ""
    weight: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Converts RuleResult instance to standard dictionary representation.

        Returns:
            Dictionary with rule outcome fields.
        """
        return {"rule_name": self.rule_name, "triggered": self.triggered, "reason": self.reason, "weight": self.weight}


class FraudRule:
    """Base class for all deterministic fraud detection rules."""

    name: str = ""

    def __init__(self, rule: RuleCfg) -> None:
        """Initializes rule with configuration parameters.

        Args:
            rule: Rule configuration dataclass.
        """
        self.rule = rule
        self.params = rule.params

    def evaluate(self, tx: dict[str, Any], features: dict[str, Any]) -> RuleResult:
        """Evaluates transaction attributes and precomputed features against rule logic.

        Args:
            tx: Raw transaction field mapping.
            features: Calculated behavioral features mapping.

        Returns:
            RuleResult outcome.

        Raises:
            NotImplementedError: If not implemented in subclass.
        """
        raise NotImplementedError

    def result(self, reason: str) -> RuleResult:
        """Builds a positive triggered RuleResult.

        Args:
            reason: Textual explanation for rule trigger.

        Returns:
            Triggered RuleResult populated with rule weight.
        """
        return RuleResult(rule_name=self.name, triggered=True, reason=reason, weight=self.rule.weight)

    def noop(self, reason: str = "") -> RuleResult:
        """Builds a non-triggered RuleResult.

        Args:
            reason: Optional explanation.

        Returns:
            Non-triggered RuleResult.
        """
        return RuleResult(rule_name=self.name, triggered=False, reason=reason, weight=self.rule.weight)


def build_rules(cfg: Config) -> list[FraudRule]:
    """Instantiates and registers all enabled fraud rules from configuration.

    Args:
        cfg: Application configuration container.

    Returns:
        List of initialized FraudRule instances.
    """
    from app.rules.bin_attack import BinAttackRule
    from app.rules.card_cloning import CardCloningRule
    from app.rules.dormant_card_wake import DormantCardWakeRule
    from app.rules.impossible_travel import ImpossibleTravelRule
    from app.rules.new_device import NewDeviceHighAmountRule
    from app.rules.time_behavior import UnusualHourAndPlaceRule
    from app.rules.unusual_amount import UnusualAmountRule

    registry: dict[str, type[FraudRule]] = {
        "card_cloning": CardCloningRule,
        "unusual_amount": UnusualAmountRule,
        "impossible_travel": ImpossibleTravelRule,
        "new_device_high_amount": NewDeviceHighAmountRule,
        "dormant_card_wake": DormantCardWakeRule,
        "bin_attack": BinAttackRule,
        "unusual_hour_and_place": UnusualHourAndPlaceRule,
    }
    rules: list[FraudRule] = []
    for name, rule_cfg in cfg.rules.items():
        if not rule_cfg.enabled or name not in registry:
            continue
        rules.append(registry[name](rule_cfg))
    return rules


def evaluate_all(rules: list[FraudRule], tx: dict[str, Any], features: dict[str, Any]) -> list[RuleResult]:
    """Executes collection of rules against single transaction context.

    Args:
        rules: Active FraudRule instances.
        tx: Raw transaction field mapping.
        features: Calculated behavioral features mapping.

    Returns:
        List of RuleResult instances for each evaluated rule.
    """
    results: list[RuleResult] = []
    for rule in rules:
        try:
            results.append(rule.evaluate(tx, features))
        except Exception as exc:
            logger.warning("rule_evaluation_failed rule=%s tx_id=%s error=%s", rule.name, tx.get("transaction_id"), exc)
            results.append(rule.noop())
    return results