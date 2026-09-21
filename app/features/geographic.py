"""Geographic features: Haversine distance, implicit travel speed, and impossible travel detection."""

from math import asin, cos, radians, sin, sqrt

EARTH_RADIUS_KM = 6371.0088

CITY_COORDS: dict[str, tuple[float, float]] = {
    "São Paulo": (-23.5505, -46.6333),
    "Rio de Janeiro": (-22.9068, -43.1729),
    "Belo Horizonte": (-19.9167, -43.9345),
    "Salvador": (-12.9714, -38.5014),
    "Recife": (-8.0476, -34.8770),
    "Fortaleza": (-3.7319, -38.5267),
    "Curitiba": (-25.4284, -49.2733),
    "Porto Alegre": (-30.0346, -51.2177),
    "Manaus": (-3.1190, -60.0217),
    "Brasília": (-15.8267, -47.9218),
    "Goiânia": (-16.6869, -49.2648),
    "Belém": (-1.4558, -48.4902),
    "Campinas": (-22.9094, -47.0626),
    "Santos": (-23.9608, -46.3336),
    "Florianópolis": (-27.5945, -48.5477),
    "Natal": (-5.7793, -35.2009),
    "João Pessoa": (-7.1195, -34.8450),
    "Cuiabá": (-15.6014, -56.0979),
    "Ribeirão Preto": (-21.1775, -47.8103),
    "Uberlândia": (-18.9186, -48.2772),
    "Londrina": (-23.3107, -51.1628),
    "Teresina": (-5.0892, -42.8019),
    "São Luís": (-2.5307, -44.3068),
    "Vitória": (-20.3155, -40.3128),
    "Campo Grande": (-20.4697, -54.6201),
    "Aracaju": (-10.9472, -37.0731),
    "Maceió": (-9.6660, -35.7350),
    "Manaus Metropolitan": (-3.1019, -60.0250),
    "Pelotas": (-31.7650, -52.3377),
    "Santarém": (-2.4431, -54.7083),
    "Foz do Iguaçu": (-25.5163, -54.5854),
}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates great-circle distance between two coordinates in kilometers.

    Args:
        lat1: Latitude of point 1 in decimal degrees.
        lon1: Longitude of point 1 in decimal degrees.
        lat2: Latitude of point 2 in decimal degrees.
        lon2: Longitude of point 2 in decimal degrees.

    Returns:
        Great-circle distance in kilometers.
    """
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    a_clamped = max(0.0, min(1.0, a))
    return 2 * EARTH_RADIUS_KM * asin(sqrt(a_clamped))


def city_distance_km(city_a: str | None, city_b: str | None) -> float:
    """Calculates Haversine distance between two known cities.

    Args:
        city_a: Name of origin city.
        city_b: Name of destination city.

    Returns:
        Distance in kilometers (0.0 if either city coordinate is unmapped).
    """
    if not city_a or not city_b or city_a not in CITY_COORDS or city_b not in CITY_COORDS:
        return 0.0
    lat1, lon1 = CITY_COORDS[city_a]
    lat2, lon2 = CITY_COORDS[city_b]
    return haversine_km(lat1, lon1, lat2, lon2)


def implicit_speed_kmh(distance_km: float, elapsed_minutes: float) -> float:
    """Calculates implied velocity in km/h between consecutive transactions.

    Args:
        distance_km: Distance in kilometers between transaction locations.
        elapsed_minutes: Time elapsed in minutes between events.

    Returns:
        Implied velocity in km/h (0.0 if elapsed time is zero or negative).
    """
    if not elapsed_minutes or elapsed_minutes <= 0:
        return 0.0
    return distance_km / (elapsed_minutes / 60.0)


def is_impossible_travel(
    distance_km: float,
    elapsed_minutes: float,
    limit_kmh: float = 900.0,
    min_distance_km: float = 50.0,
) -> bool:
    """Evaluates whether implied velocity between two transactions violates physical travel limits.

    Args:
        distance_km: Distance in kilometers.
        elapsed_minutes: Elapsed time in minutes.
        limit_kmh: Maximum realistic travel speed threshold in km/h.
        min_distance_km: Minimum distance threshold to trigger evaluation.

    Returns:
        True if distance exceeds min_distance_km and implied velocity exceeds limit_kmh.
    """
    if distance_km < min_distance_km:
        return False
    speed = implicit_speed_kmh(distance_km, elapsed_minutes)
    return speed > limit_kmh