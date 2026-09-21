"""PySpark StructType schema definitions for enriched transactions and features."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from app.schemas.transaction import TRANSACTION_SCHEMA


FEATURE_FIELDS = [
    ("velocity_1min", LongType()),
    ("velocity_5min", LongType()),
    ("velocity_1h", LongType()),
    ("velocity_24h", LongType()),
    ("count_10min", LongType()),
    ("sum_10min", DoubleType()),
    ("min_10min", DoubleType()),
    ("amount_zscore", DoubleType()),
    ("amount_vs_avg_ratio", DoubleType()),
    ("distance_from_last_km", DoubleType()),
    ("travel_speed_kmh", DoubleType()),
    ("minutes_since_last", DoubleType()),
    ("dormant_days", DoubleType()),
    ("new_terminal", BooleanType()),
    ("new_device", BooleanType()),
    ("new_city", BooleanType()),
    ("entry_mode_chip_only_history", BooleanType()),
    ("high_risk_merchant", BooleanType()),
    ("hour_of_day", LongType()),
    ("bin_denials_15min", LongType()),
    ("previous_transaction_id", StringType()),
]

DECISION_FIELDS = [
    ("risk_score", DoubleType()),
    ("risk_level", StringType()),
    ("triggered_rules", ArrayType(StringType())),
    ("rule_reasons", ArrayType(StringType())),
    ("decision_timestamp", TimestampType()),
    ("model_version", StringType()),
]

STORED_RAW_FIELDS = [
    StructField(name, typ, True)
    for name, typ in [
        ("transaction_id", StringType()),
        ("customer_id", StringType()),
        ("card_id", StringType()),
        ("timestamp", TimestampType()),
        ("amount", DoubleType()),
        ("currency", StringType()),
        ("merchant_id", StringType()),
        ("merchant_category", StringType()),
        ("merchant_city", StringType()),
        ("merchant_country", StringType()),
        ("terminal_id", StringType()),
        ("entry_mode", StringType()),
        ("device_id", StringType()),
        ("channel", StringType()),
        ("auth_result", StringType()),
    ]
]

ENRICHED_SCHEMA = StructType(
    [f for f in TRANSACTION_SCHEMA.fields if f.name != "dt"]
    + [StructField(name, typ, True) for name, typ in FEATURE_FIELDS]
    + [StructField(name, typ, True) for name, typ in DECISION_FIELDS]
)

STORED_ENRICHED_SCHEMA = StructType(
    STORED_RAW_FIELDS
    + [StructField(name, typ, True) for name, typ in FEATURE_FIELDS]
    + [StructField(name, typ, True) for name, typ in DECISION_FIELDS]
)