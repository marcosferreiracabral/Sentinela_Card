"""PySpark StructType schema definitions and field constants for raw transactions."""

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_FIELDS: list[tuple[str, StringType | DoubleType | TimestampType]] = [
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

RAW_FIELD_NAMES: list[str] = [name for name, _ in RAW_FIELDS]
RAW_IDENTITY_COLUMNS: list[str] = ["card_id", "transaction_id", "timestamp", "amount"]

TRANSACTION_SCHEMA = StructType([StructField(name, typ, True) for name, typ in RAW_FIELDS])

STORED_RAW_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("merchant_city", StringType(), True),
        StructField("merchant_country", StringType(), True),
        StructField("terminal_id", StringType(), True),
        StructField("entry_mode", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("auth_result", StringType(), True),
    ]
)

ENTRY_MODES: list[str] = ["chip", "contactless", "magnetic", "manual", "ecommerce"]
CHANNELS: list[str] = ["pos", "atm", "online"]
AUTH_RESULTS: list[str] = ["approved", "declined"]


def bin_from_card(card_id: str) -> str:
    """Extracts first 6 numeric digits (Bank Identification Number) from card identifier.

    Args:
        card_id: Card account number or identifier.

    Returns:
        First 6 digits representing the BIN.
    """
    digits = "".join(ch for ch in card_id if ch.isdigit())
    return digits[:6] or card_id[:6]
