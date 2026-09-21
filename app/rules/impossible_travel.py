"""Impossible travel detection rule: flags consecutive transactions across distant cities with unrealistic implied velocities."""

from typing import Any

from app.rules.engine import FraudRule, RuleResult


class ImpossibleTravelRule(FraudRule):
    """Detects physically impossible travel velocity between consecutive card transactions."""

    name: str = "impossible_travel"

    def evaluate(self, tx: dict[str, Any], features: dict[str, Any]) -> RuleResult:
        """Evaluates geographic distance and implied travel velocity between events.

        Args:
            tx: Raw transaction attributes.
            features: Precalculated spatial distance and speed features.

        Returns:
            RuleResult with evaluation verdict and reason.
        """
        limit_kmh = float(self.params.get("speed_limit_kmh", 900.0))
        min_distance = float(self.params.get("min_distance_km", 50.0))
        distance = features.get("distance_from_last_km") or 0.0
        speed = features.get("travel_speed_kmh") or 0.0
        prev_city = features.get("prev_merchant_city")
        current_city = tx.get("merchant_city")

        if distance < min_distance or speed <= 0:
            return self.noop()
        if speed > limit_kmh:
            mins = features.get("minutes_since_last", 0)
            return self.result(
                f"deslocamento impossível: {prev_city} → {current_city} ({distance:.0f} km em {mins:.0f}min = {speed:.0f} km/h)"
            )
        return self.noop()