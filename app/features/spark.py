"""Temporal sliding window and historical state feature computation using PySpark."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

ONE_MIN: int = 60
FIVE_MIN: int = 300
ONE_HOUR: int = 3600
ONE_DAY: int = 86400
TEN_MIN: int = 600
NINETY_DAYS: int = 7776000
BIN_WINDOW_SEC: int = 900

CHIP_MODES: tuple[str, ...] = ("chip", "contactless")

SPARK_FEATURE_COLUMNS: list[str] = [
    "velocity_1min",
    "velocity_5min",
    "velocity_1h",
    "velocity_24h",
    "count_10min",
    "sum_10min",
    "min_10min",
    "amount_mean_90d",
    "amount_std_90d",
    "amount_count_90d",
    "prev_ts",
    "prev_merchant_city",
    "prev_terminal_id",
    "prev_device_id",
    "prev_amount",
    "prev_entry_mode",
    "prev_transaction_id",
    "bin_denials_15min",
    "prev_bin_approved",
    "new_terminal",
    "new_device",
    "new_city",
    "entry_mode_chip_only_history",
]


def _align(df: DataFrame) -> DataFrame:
    """Projects and casts standard columns required for feature window processing.

    Args:
        df: Input raw or historical transactions DataFrame.

    Returns:
        Standardized DataFrame with extracted epoch timestamp and BIN partition keys.
    """
    return df.select(
        F.col("transaction_id"),
        F.col("customer_id"),
        F.col("card_id"),
        F.col("timestamp").alias("ts"),
        F.unix_timestamp("timestamp").cast("long").alias("ts_epoch"),
        F.col("amount").cast("double").alias("amount"),
        F.col("currency"),
        F.col("merchant_id"),
        F.col("merchant_category"),
        F.col("merchant_city"),
        F.col("merchant_country"),
        F.col("terminal_id"),
        F.col("entry_mode"),
        F.col("device_id"),
        F.col("channel"),
        F.col("auth_result"),
        F.substring(F.col("card_id"), 1, 6).alias("bin"),
    )


def _empty(spark: SparkSession) -> DataFrame:
    """Builds an empty DataFrame matching the aligned feature schema.

    Args:
        spark: Active SparkSession.

    Returns:
        Empty DataFrame with aligned schema.
    """
    cols = [
        "transaction_id",
        "customer_id",
        "card_id",
        "ts",
        "ts_epoch",
        "amount",
        "currency",
        "merchant_id",
        "merchant_category",
        "merchant_city",
        "merchant_country",
        "terminal_id",
        "entry_mode",
        "device_id",
        "channel",
        "auth_result",
        "bin",
    ]
    schema = StructType([StructField(c, StringType(), True) for c in cols])
    import pandas as pd

    return spark.createDataFrame(pd.DataFrame([], columns=cols), schema=schema)


def _has_rows(df: DataFrame | None) -> bool:
    """Checks whether DataFrame is non-null and possesses valid schema columns.

    Args:
        df: Target DataFrame or None.

    Returns:
        True if DataFrame exists with defined schema names.
    """
    return df is not None and bool(df.schema.names)


def attach_spark_features(batch: DataFrame, history: DataFrame | None, spark: SparkSession) -> DataFrame:
    """Computes sliding window metrics and stateful historical indicators for a micro-batch.

    Args:
        batch: Current batch DataFrame of raw incoming transactions.
        history: Prior historical transactions DataFrame or None.
        spark: Active SparkSession.

    Returns:
        DataFrame enriched with temporal sliding windows, lag features, and novelty flags.
    """
    batch_aligned = _align(batch).withColumn("_src", F.lit("batch"))
    if history is not None and _has_rows(history):
        hist_aligned = _align(history).withColumn("_src", F.lit("history"))
        combined = hist_aligned.unionByName(batch_aligned)
    else:
        combined = batch_aligned

    w_card = Window.partitionBy("card_id").orderBy("ts_epoch")
    w_cust = Window.partitionBy("customer_id").orderBy("ts_epoch")
    w_bin = Window.partitionBy("bin").orderBy("ts_epoch")

    combined = combined.withColumn("velocity_1min", F.count(F.lit(1)).over(w_card.rangeBetween(-ONE_MIN, -1)))
    combined = combined.withColumn("velocity_5min", F.count(F.lit(1)).over(w_card.rangeBetween(-FIVE_MIN, -1)))
    combined = combined.withColumn("velocity_1h", F.count(F.lit(1)).over(w_card.rangeBetween(-ONE_HOUR, -1)))
    combined = combined.withColumn("velocity_24h", F.count(F.lit(1)).over(w_card.rangeBetween(-ONE_DAY, -1)))
    combined = combined.withColumn("count_10min", F.count(F.lit(1)).over(w_card.rangeBetween(-TEN_MIN, -1)))
    combined = combined.withColumn("sum_10min", F.coalesce(F.sum("amount").over(w_card.rangeBetween(-TEN_MIN, -1)), F.lit(0.0)))
    combined = combined.withColumn("min_10min", F.coalesce(F.min("amount").over(w_card.rangeBetween(-TEN_MIN, -1)), F.lit(0.0)))
    combined = combined.withColumn("amount_mean_90d", F.coalesce(F.avg("amount").over(w_cust.rangeBetween(-NINETY_DAYS, -1)), F.lit(0.0)))
    combined = combined.withColumn(
        "amount_std_90d", F.coalesce(F.stddev_samp("amount").over(w_cust.rangeBetween(-NINETY_DAYS, -1)), F.lit(0.0))
    )
    combined = combined.withColumn("amount_count_90d", F.coalesce(F.count("amount").over(w_cust.rangeBetween(-NINETY_DAYS, -1)), F.lit(0)))
    combined = combined.withColumn(
        "bin_denials_15min",
        F.sum(F.when(F.col("auth_result") == "declined", 1).otherwise(0)).over(w_bin.rangeBetween(-BIN_WINDOW_SEC, -1)),
    )

    w_prev = Window.partitionBy("card_id").orderBy("ts_epoch", "transaction_id")
    combined = combined.withColumn("prev_ts", F.lag("ts").over(w_prev))
    combined = combined.withColumn("prev_merchant_city", F.lag("merchant_city").over(w_prev))
    combined = combined.withColumn("prev_terminal_id", F.lag("terminal_id").over(w_prev))
    combined = combined.withColumn("prev_device_id", F.lag("device_id").over(w_prev))
    combined = combined.withColumn("prev_amount", F.lag("amount").over(w_prev))
    combined = combined.withColumn("prev_entry_mode", F.lag("entry_mode").over(w_prev))
    combined = combined.withColumn("prev_transaction_id", F.lag("transaction_id").over(w_prev))

    w_bin_prev = Window.partitionBy("bin").orderBy("ts_epoch", "transaction_id")
    combined = combined.withColumn("prev_bin_approved", F.when(F.lag("auth_result").over(w_bin_prev) == "approved", True).otherwise(False))

    tx_features_df = combined.filter(F.col("_src") == "batch").drop("_src", "ts_epoch", "bin")

    if history is not None and _has_rows(history):
        for flag_col, base_col in [("new_terminal", "terminal_id"), ("new_device", "device_id"), ("new_city", "merchant_city")]:
            seen = (
                F.broadcast(
                    history.select(F.col("card_id").alias("seen_card_id"), F.col(base_col).alias("seen_value"))
                    .filter(F.col("seen_value").isNotNull())
                    .distinct()
                )
            )
            tx_features_df = tx_features_df.join(
                seen,
                (tx_features_df["card_id"] == seen["seen_card_id"]) & (tx_features_df[base_col] == seen["seen_value"]),
                "left",
            )
            tx_features_df = tx_features_df.withColumn(flag_col, F.col("seen_value").isNull()).drop("seen_value", "seen_card_id")

        non_chip = F.broadcast(
            history.filter(~F.col("entry_mode").isin(list(CHIP_MODES))).select("card_id").distinct().withColumnRenamed("card_id", "non_chip_card")
        )
        tx_features_df = tx_features_df.join(non_chip, tx_features_df["card_id"] == non_chip["non_chip_card"], "left")
        tx_features_df = tx_features_df.withColumn("entry_mode_chip_only_history", F.col("non_chip_card").isNull()).drop("non_chip_card")
    else:
        for flag_col, _ in [("new_terminal", "terminal_id"), ("new_device", "device_id"), ("new_city", "merchant_city")]:
            tx_features_df = tx_features_df.withColumn(flag_col, F.lit(True))
        tx_features_df = tx_features_df.withColumn("entry_mode_chip_only_history", F.lit(False))

    return tx_features_df