"""Testes unitários das features comportamentais."""

from datetime import datetime, timezone

from app.features.amount_profile import (
    amount_vs_avg_ratio,
    amount_zscore,
    is_unusual_amount,
    mean_std,
)
from app.features.device_fingerprint import is_new_device, same_card_two_devices
from app.features.geographic import (
    city_distance_km,
    haversine_km,
    implicit_speed_kmh,
    is_impossible_travel,
)
from app.features.merchant_risk import is_high_risk_merchant
from app.features.time_behavior import (
    dormant_days_since,
    is_dormant_card,
    is_untrusted_hour,
)
from app.features.velocity import (
    count_window,
    is_high_velocity,
    sliding_windows_are_monotonic,
    sum_window,
)


class TestVelocityFeatures:
    def test_count_window_basic(self):
        # Eventos em timestamps (segundos): 10, 40, 55, 65, 80. Current = 70.
        # Janela de 60s -> intervalo [10, 70). Eventos: 10, 40, 55, 65 = 4 eventos.
        times = [10, 40, 55, 65, 80]
        c = count_window(times, current=70, window_seconds=60)
        assert c == 4

    def test_sum_window_basic(self):
        amounts = [10.0, 20.0, 30.0, 50.0]
        times = [10, 20, 50, 90]
        s = sum_window(amounts, times, current=60, window_seconds=60)
        assert s == 60.0  # 10 + 20 + 30

    def test_sliding_windows_monotonicity(self):
        assert sliding_windows_are_monotonic(c1=2, c5=4, c60=10, c1440=15) is True
        assert sliding_windows_are_monotonic(c1=5, c5=4, c60=10, c1440=15) is False

    def test_high_velocity_flag(self):
        assert is_high_velocity(count=6, window_seconds=60, limit=5) is True
        assert is_high_velocity(count=3, window_seconds=60, limit=5) is False


class TestGeographicFeatures:
    def test_haversine_known_distance(self):
        # São Paulo (-23.5505, -46.6333) -> Rio de Janeiro (-22.9068, -43.1729) ~ 360 km
        dist = haversine_km(-23.5505, -46.6333, -22.9068, -43.1729)
        assert 340 < dist < 380

    def test_city_distance_km(self):
        dist = city_distance_km("São Paulo", "Rio de Janeiro")
        assert 340 < dist < 380
        # Cidade desconhecida
        assert city_distance_km("Cidade Fantasma", "São Paulo") == 0.0

    def test_implicit_speed_calculation(self):
        # 500 km em 30 minutos (0.5 h) = 1000 km/h
        speed = implicit_speed_kmh(distance_km=500.0, elapsed_minutes=30.0)
        assert speed == 1000.0
        # Tempo zero ou negativo
        assert implicit_speed_kmh(500.0, 0) == 0.0

    def test_impossible_travel_flag(self):
        # 800 km em 20 min -> 2400 km/h (> 900 km/h limit)
        assert is_impossible_travel(distance_km=800.0, elapsed_minutes=20.0) is True
        # 100 km em 120 min -> 50 km/h
        assert is_impossible_travel(distance_km=100.0, elapsed_minutes=120.0) is False
        # Distância curta (< 50 km) não deve disparar impossible travel
        assert is_impossible_travel(distance_km=30.0, elapsed_minutes=1.0) is False


class TestAmountProfileFeatures:
    def test_mean_std_calculation(self):
        values = [100.0, 100.0, 100.0, 100.0]
        mean, std = mean_std(values)
        assert mean == 100.0
        assert std == 0.0

    def test_amount_zscore(self):
        # Valor 300 com média 100 e desvio 50 -> z = (300-100)/50 = 4.0
        z = amount_zscore(value=300.0, mean=100.0, std=50.0)
        assert z == 4.0
        # std quase zero não deve causar ZeroDivisionError
        assert amount_zscore(value=300.0, mean=100.0, std=0.0) == 0.0

    def test_amount_vs_avg_ratio(self):
        ratio = amount_vs_avg_ratio(value=350.0, mean=100.0)
        assert ratio == 3.5
        # Média inexistente ou zero
        assert amount_vs_avg_ratio(value=350.0, mean=0.0) == 1.0

    def test_is_unusual_amount(self):
        assert is_unusual_amount(value=400.0, mean=100.0, std=50.0, zscore_min=3.0, ratio_min=1.5) is True
        assert is_unusual_amount(value=110.0, mean=100.0, std=50.0, zscore_min=3.0, ratio_min=1.5) is False


class TestMerchantRiskFeatures:
    def test_high_risk_categories(self):
        assert is_high_risk_merchant("7995") is True   # Apostas
        assert is_high_risk_merchant("6051") is True   # Câmbio / crypto
        assert is_high_risk_merchant("5411") is False  # Supermercado
        assert is_high_risk_merchant(None) is False


class TestDeviceFingerprintFeatures:
    def test_is_new_device(self):
        seen = {"DEV-001", "DEV-002"}
        assert is_new_device("DEV-003", seen) is True
        assert is_new_device("DEV-001", seen) is False
        assert is_new_device(None, seen) is False

    def test_same_card_two_devices(self):
        assert same_card_two_devices("DEV-001", "DEV-002") is True
        assert same_card_two_devices("DEV-001", "DEV-001") is False
        assert same_card_two_devices(None, "DEV-001") is False


class TestTimeBehaviorFeatures:
    def test_untrusted_hour(self):
        assert is_untrusted_hour(3, start=2, end=5) is True
        assert is_untrusted_hour(14, start=2, end=5) is False
        assert is_untrusted_hour(8, start=2, end=5) is False

    def test_dormant_days_and_flag(self):
        dt1 = datetime(2025, 1, 1, tzinfo=timezone.utc)
        dt2 = datetime(2025, 8, 1, tzinfo=timezone.utc)
        days = dormant_days_since(dt1, dt2)
        assert days > 200
        assert is_dormant_card(days, limit=180.0) is True
        assert is_dormant_card(10.0, limit=180.0) is False
