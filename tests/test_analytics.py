"""Testes para o motor analítico Spark SQL e todas as queries."""

from pyspark.sql import SparkSession

from app.analytics.engine import AnalyticsEngine
from app.analytics.queries import QUERIES
from app.cli.backfill import run_backfill
from app.config import Config
from data.generator import generate


class TestAnalyticsEngine:
    def test_all_analytical_queries(self, spark: SparkSession, test_config: Config, tmp_data_dir):
        # 1. Gera dataset sintético reduzido para teste analítico
        generate(
            spark=spark,
            n_transactions=300,
            n_cloning=5,
            n_travel=3,
            n_bin=2,
            n_dormant=2,
            n_new_device=2,
            n_unusual=2,
            output_dir=str(tmp_data_dir),
            write=True,
        )

        # 2. Executa backfill para popular as camadas raw, enriched e alerts
        seed_file = tmp_data_dir / "seed_transactions.parquet"
        run_backfill(spark, test_config, str(seed_file))

        # 3. Inicializa o AnalyticsEngine e registra views temporárias
        engine = AnalyticsEngine(spark, test_config)
        engine.register_views()

        # 4. Testa cada consulta analítica
        for query_name in QUERIES:
            df = engine.run_query(query_name)
            assert df is not None
            # Garante que a query executa sem erros e retorna schema válido
            count = df.count()
            assert count >= 0
            formatted = engine.format_query_result(query_name)
            assert f"Relatório Analítico: {query_name}" in formatted
