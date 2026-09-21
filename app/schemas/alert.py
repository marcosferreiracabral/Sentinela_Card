"""PySpark StructType schema definition for generated fraud alerts."""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ALERT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("bin", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("merchant_city", StringType(), True),
        StructField("merchant_country", StringType(), True),
        StructField("entry_mode", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("risk_score", DoubleType(), True),
        StructField("risk_level", StringType(), True),
        StructField("triggered_rules", ArrayType(StringType()), True),
        StructField("rule_reasons", ArrayType(StringType()), True),
        StructField("feature_snapshot", StringType(), True),
        StructField("decision_timestamp", TimestampType(), True),
        StructField("model_version", StringType(), True),
    ]
)