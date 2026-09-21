"""Testes unitários para combinação de scores e limiares de decisão."""

from app.config import Thresholds
from app.rules.engine import RuleResult
from app.scoring.combiner import combine
from app.scoring.threshold import decide


class TestScoringAndThreshold:
    def test_combine_rules_sum(self):
        results = [
            RuleResult(rule_name="card_cloning", triggered=True, reason="Teste seguido de compra grande", weight=55.0),
            RuleResult(rule_name="unusual_amount", triggered=True, reason="Z-score alto", weight=35.0),
            RuleResult(rule_name="new_device", triggered=False, reason="", weight=45.0),
        ]
        score, triggered_rules, reasons = combine(results, max_score=100.0)
        assert score == 90.0
        assert triggered_rules == ["card_cloning", "unusual_amount"]
        assert len(reasons) == 2

    def test_combine_max_score_cap(self):
        results = [
            RuleResult(rule_name="impossible_travel", triggered=True, reason="Velocidade > 900 km/h", weight=80.0),
            RuleResult(rule_name="card_cloning", triggered=True, reason="Clonagem", weight=55.0),
        ]
        score, triggered_rules, _ = combine(results, max_score=100.0)
        assert score == 100.0  # Cap em 100

    def test_threshold_decisions(self):
        thresholds = Thresholds(approve_below=30, review_from=30, block_from=70)

        assert decide(0.0, thresholds) == "approve"
        assert decide(25.0, thresholds) == "approve"
        assert decide(29.9, thresholds) == "approve"

        assert decide(30.0, thresholds) == "review"
        assert decide(55.0, thresholds) == "review"
        assert decide(69.9, thresholds) == "review"

        assert decide(70.0, thresholds) == "block"
        assert decide(85.0, thresholds) == "block"
        assert decide(100.0, thresholds) == "block"
