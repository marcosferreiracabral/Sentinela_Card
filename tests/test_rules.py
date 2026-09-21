"""Testes unitários para cada regra individual de detecção de fraude."""

from app.config import load_config
from app.rules.bin_attack import BinAttackRule
from app.rules.card_cloning import CardCloningRule
from app.rules.dormant_card_wake import DormantCardWakeRule
from app.rules.engine import build_rules, evaluate_all
from app.rules.impossible_travel import ImpossibleTravelRule
from app.rules.new_device import NewDeviceHighAmountRule
from app.rules.time_behavior import UnusualHourAndPlaceRule
from app.rules.unusual_amount import UnusualAmountRule


class TestFraudRules:
    def setup_method(self):
        self.cfg = load_config("rules.yaml")

    def test_card_cloning_rule_test_then_big(self):
        rule_cfg = self.cfg.rules["card_cloning"]
        rule = CardCloningRule(rule_cfg)

        # Transação alta após teste de R$ 5,00 em 3 minutos na mesma cidade
        tx = {
            "amount": 1500.0,
            "merchant_city": "São Paulo",
            "entry_mode": "chip",
            "terminal_id": "T002",
        }
        features = {
            "count_10min": 2,
            "min_10min": 5.0,
            "minutes_since_last": 3.0,
            "prev_merchant_city": "São Paulo",
            "new_terminal": True,
            "entry_mode_chip_only_history": True,
        }
        res = rule.evaluate(tx, features)
        assert res.triggered is True
        assert res.weight == rule_cfg.weight
        assert "teste R$" in res.reason

    def test_card_cloning_magnetic_stripe_on_chip_card(self):
        rule_cfg = self.cfg.rules["card_cloning"]
        rule = CardCloningRule(rule_cfg)

        tx = {
            "amount": 200.0,
            "merchant_city": "São Paulo",
            "entry_mode": "magnetic",
        }
        features = {
            "count_10min": 0,
            "min_10min": 0.0,
            "minutes_since_last": 100.0,
            "prev_merchant_city": "São Paulo",
            "new_terminal": False,
            "entry_mode_chip_only_history": True,
        }
        res = rule.evaluate(tx, features)
        assert res.triggered is True
        assert "tarja magnética" in res.reason

    def test_impossible_travel_rule(self):
        rule_cfg = self.cfg.rules["impossible_travel"]
        rule = ImpossibleTravelRule(rule_cfg)

        # SP -> Manaus (2690 km) em 10 min -> ~16.000 km/h
        tx = {"merchant_city": "Manaus"}
        features = {
            "distance_from_last_km": 2690.0,
            "travel_speed_kmh": 16140.0,
            "minutes_since_last": 10.0,
            "prev_merchant_city": "São Paulo",
        }
        res = rule.evaluate(tx, features)
        assert res.triggered is True
        assert "São Paulo → Manaus" in res.reason

        # Viagem normal: 300 km em 300 min = 60 km/h
        features_ok = {
            "distance_from_last_km": 300.0,
            "travel_speed_kmh": 60.0,
            "minutes_since_last": 300.0,
            "prev_merchant_city": "São Paulo",
        }
        res_ok = rule.evaluate(tx, features_ok)
        assert res_ok.triggered is False

    def test_unusual_amount_rule(self):
        rule_cfg = self.cfg.rules["unusual_amount"]
        rule = UnusualAmountRule(rule_cfg)

        tx = {"amount": 2500.0}
        features = {
            "amount_zscore": 6.0,
            "amount_vs_avg_ratio": 5.0,
            "amount_count_90d": 20,
            "amount_mean_90d": 500.0,
        }
        res = rule.evaluate(tx, features)
        assert res.triggered is True
        assert "z=6.0" in res.reason

        # Transação comum dentro da média
        features_norm = {
            "amount_zscore": 0.2,
            "amount_vs_avg_ratio": 1.05,
            "amount_count_90d": 20,
            "amount_mean_90d": 500.0,
        }
        res_norm = rule.evaluate(tx, features_norm)
        assert res_norm.triggered is False

    def test_new_device_high_amount_rule(self):
        rule_cfg = self.cfg.rules["new_device_high_amount"]
        rule = NewDeviceHighAmountRule(rule_cfg)

        tx = {"amount": 1800.0, "device_id": "DEV-UNKNOWN-999"}
        features = {
            "new_device": True,
            "amount_mean_90d": 200.0,
        }
        res = rule.evaluate(tx, features)
        assert res.triggered is True
        assert "nunca visto" in res.reason

        # Dispositivo já conhecido
        features_known = {
            "new_device": False,
            "amount_mean_90d": 200.0,
        }
        res_known = rule.evaluate(tx, features_known)
        assert res_known.triggered is False

    def test_dormant_card_wake_rule(self):
        rule_cfg = self.cfg.rules["dormant_card_wake"]
        rule = DormantCardWakeRule(rule_cfg)

        tx = {"amount": 2200.0}
        features = {
            "dormant_days": 210.0,
            "amount_mean_90d": 400.0,
        }
        res = rule.evaluate(tx, features)
        assert res.triggered is True
        assert "cartão inativo 210 dias" in res.reason

        # Cartão ativo (10 dias desde última transação)
        features_active = {
            "dormant_days": 10.0,
            "amount_mean_90d": 400.0,
        }
        res_active = rule.evaluate(tx, features_active)
        assert res_active.triggered is False

    def test_bin_attack_rule(self):
        rule_cfg = self.cfg.rules["bin_attack"]
        rule = BinAttackRule(rule_cfg)

        tx = {"card_id": "4532019999999999"}
        features = {
            "bin_denials_15min": 4,
            "prev_bin_approved": False,
        }
        res = rule.evaluate(tx, features)
        assert res.triggered is True
        assert "4 negadas em 453201" in res.reason

        # Poucas negadas (apenas 1)
        features_ok = {
            "bin_denials_15min": 1,
            "prev_bin_approved": False,
        }
        assert rule.evaluate(tx, features_ok).triggered is False

    def test_unusual_hour_and_place_rule(self):
        rule_cfg = self.cfg.rules["unusual_hour_and_place"]
        rule = UnusualHourAndPlaceRule(rule_cfg)

        tx = {"merchant_city": "Fortaleza", "amount": 350.0}
        features = {
            "hour_of_day": 3,
            "new_city": True,
            "prev_merchant_city": "São Paulo",
        }
        res = rule.evaluate(tx, features)
        assert res.triggered is True
        assert "Fortaleza" in res.reason

        # Madrugada na própria cidade
        features_home = {
            "hour_of_day": 3,
            "new_city": False,
            "prev_merchant_city": "Fortaleza",
        }
        assert rule.evaluate(tx, features_home).triggered is False

    def test_engine_evaluate_all(self):
        rules = build_rules(self.cfg)
        assert len(rules) >= 7

        tx = {"amount": 25.0, "merchant_city": "São Paulo", "entry_mode": "chip"}
        features = {"amount_mean_90d": 30.0, "new_terminal": False}
        results = evaluate_all(rules, tx, features)
        assert len(results) == len(rules)
        # Nenhuma regra deve disparar para transação normal
        assert all(not r.triggered for r in results)
