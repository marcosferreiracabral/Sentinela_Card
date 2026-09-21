"""Spark SQL analytics execution engine."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from app.analytics.queries import QUERIES
from app.config import Config
from app.storage.reader import read_alerts, read_enriched, read_raw


class AnalyticsEngine:
    """Loads persisted Parquet data as Spark SQL temporary views and executes analytical queries."""

    def __init__(self, spark: SparkSession, cfg: Config) -> None:
        """Initializes analytics engine with SparkSession and application configuration.

        Args:
            spark: Active SparkSession.
            cfg: Application configuration container.
        """
        self.spark = spark
        self.cfg = cfg
        self._views_registered = False

    def register_views(self) -> None:
        """Registers persisted data layers and ground-truth metadata as temporary Spark views."""
        from pyspark.sql.types import BooleanType, StringType, StructField, StructType

        from app.schemas.alert import ALERT_SCHEMA
        from app.schemas.enriched_transaction import STORED_ENRICHED_SCHEMA
        from app.schemas.transaction import STORED_RAW_SCHEMA

        raw_df = read_raw(self.cfg, self.spark)
        if raw_df is not None:
            raw_df.createOrReplaceTempView("raw")
        else:
            self.spark.createDataFrame(
                self.spark.sparkContext.emptyRDD(), schema=STORED_RAW_SCHEMA
            ).createOrReplaceTempView("raw")

        enriched_df = read_enriched(self.cfg, self.spark)
        if enriched_df is not None:
            enriched_df.createOrReplaceTempView("enriched")
        else:
            self.spark.createDataFrame(
                self.spark.sparkContext.emptyRDD(), schema=STORED_ENRICHED_SCHEMA
            ).createOrReplaceTempView("enriched")

        alerts_df = read_alerts(self.cfg, self.spark)
        if alerts_df is not None:
            alerts_df.createOrReplaceTempView("alerts")
        else:
            self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema=ALERT_SCHEMA).createOrReplaceTempView(
                "alerts"
            )

        labels_path = Path(self.cfg.data_dir) / "metadata" / "fraud_labels.parquet"
        if labels_path.exists():
            labels_df = self.spark.read.parquet(str(labels_path))
            labels_df.createOrReplaceTempView("fraud_labels")
        else:
            fraud_label_schema = StructType(
                [
                    StructField("transaction_id", StringType(), True),
                    StructField("is_fraud", BooleanType(), True),
                    StructField("fraud_type", StringType(), True),
                ]
            )
            self.spark.createDataFrame(
                self.spark.sparkContext.emptyRDD(), schema=fraud_label_schema
            ).createOrReplaceTempView("fraud_labels")

        self._views_registered = True

    def run_query(self, query_name_or_sql: str) -> DataFrame:
        """Executes predefined named query or custom SQL statement.

        Args:
            query_name_or_sql: Name of predefined query from catalog or raw SQL string.

        Returns:
            Spark DataFrame containing query execution results.
        """
        if not self._views_registered:
            self.register_views()

        sql_text = QUERIES.get(query_name_or_sql, query_name_or_sql)
        return self.spark.sql(sql_text)

    def format_query_result(self, query_name: str, num_rows: int = 50) -> str:
        """Formats query results into tabular string for presentation.

        Args:
            query_name: Predefined query name or SQL string.
            num_rows: Maximum number of rows to include in output.

        Returns:
            Formatted tabular representation of result set.
        """
        df = self.run_query(query_name)
        pdf = df.limit(num_rows).toPandas()
        if pdf.empty:
            return f"Query '{query_name}' returned no records."

        header = f"\n=== Relatório Analítico: {query_name} ===\n"
        table_str = pdf.to_string(index=False)
        return header + table_str + f"\n\nTotal rows displayed: {len(pdf)}\n"


def run_analysis(spark: SparkSession, cfg: Config, query_name: str, show: bool = True) -> DataFrame:
    """Executes query through AnalyticsEngine and optionally displays formatted output.

    Args:
        spark: Active SparkSession.
        cfg: Application configuration container.
        query_name: Predefined query name or SQL string.
        show: If True, prints formatted tabular results to stdout.

    Returns:
        Spark DataFrame of query results.
    """
    engine = AnalyticsEngine(spark, cfg)
    engine.register_views()
    result_df = engine.run_query(query_name)
    if show:
        print(engine.format_query_result(query_name))
    return result_df
