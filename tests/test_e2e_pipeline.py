"""Teste End-to-End do Pipeline Antifraude Sentinela_Card.

Valida os critérios de aceite:
  1. Sequência de clonagem (teste < R$10 seguido de compra grande) é bloqueada.
  2. Viagem impossível (cidades distantes em tempo impossível) é bloqueada.
  3. Transação normal de padaria às 08h da manhã é aprovada (score < 30).
  4. Motivo e score aparecem para cada alerta.
  5. Taxa de falsos positivos em transações puramente normais < 2%.
"""

from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from app.config import Config
from app.schemas.transaction import TRANSACTION_SCHEMA
from app.streaming.processor import FraudPipeline
from data.generator import generate


class TestEndToEndPipeline:
    def test_e2e_card_cloning_blocked(self, spark: SparkSession, test_config: Config):
        pipeline = FraudPipeline(spark, test_config, write=False)
        base_time = datetime(2026, 3, 10, 14, 0, 0, tzinfo=timezone.utc)

        # Transação 1: Teste de valor baixo (R$ 4,50)
        # Transação 2: Compra alta 4 minutos depois (R$ 2.800,00) na mesma cidade
        cloning_txs = [
            {
                "transaction_id": "tx_clone_test_01",
                "customer_id": "C_CLONE_01",
                "card_id": "4532018888888888",
                "timestamp": base_time,
                "amount": 4.50,
                "currency": "BRL",
                "merchant_id": "M_TEST_01",
                "merchant_category": "5999",
                "merchant_city": "São Paulo",
                "merchant_country": "BR",
                "terminal_id": "T_TERM_01",
                "entry_mode": "magnetic",
                "device_id": None,
                "channel": "pos",
                "auth_result": "approved",
            },
            {
                "transaction_id": "tx_clone_big_02",
                "customer_id": "C_CLONE_01",
                "card_id": "4532018888888888",
                "timestamp": base_time + timedelta(minutes=4),
                "amount": 2800.00,
                "currency": "BRL",
                "merchant_id": "M_ELETRO_02",
                "merchant_category": "5732",
                "merchant_city": "São Paulo",
                "merchant_country": "BR",
                "terminal_id": "T_TERM_02",
                "entry_mode": "magnetic",
                "device_id": None,
                "channel": "pos",
                "auth_result": "approved",
            },
        ]

        import pandas as pd

        df = spark.createDataFrame(pd.DataFrame(cloning_txs), schema=TRANSACTION_SCHEMA)
        res = pipeline.process_batch(df)

        pdf = res.enriched.toPandas()
        big_tx_res = pdf[pdf["transaction_id"] == "tx_clone_big_02"].iloc[0]

        # A transação grande deve ser BLOQUEADA
        assert big_tx_res["risk_level"] == "block"
        assert big_tx_res["risk_score"] >= 70
        assert "card_cloning" in big_tx_res["triggered_rules"]
        assert len(big_tx_res["rule_reasons"]) > 0

    def test_e2e_impossible_travel_blocked(self, spark: SparkSession, test_config: Config):
        pipeline = FraudPipeline(spark, test_config, write=False)
        base_time = datetime(2026, 3, 10, 10, 0, 0, tzinfo=timezone.utc)

        # Transação 1: Compra em São Paulo
        # Transação 2: 12 minutos depois em Manaus (distância ~2690 km)
        travel_txs = [
            {
                "transaction_id": "tx_travel_sp_01",
                "customer_id": "C_TRAVEL_01",
                "card_id": "4024007777777777",
                "timestamp": base_time,
                "amount": 50.00,
                "currency": "BRL",
                "merchant_id": "M_SP_01",
                "merchant_category": "5411",
                "merchant_city": "São Paulo",
                "merchant_country": "BR",
                "terminal_id": "T_SP_01",
                "entry_mode": "chip",
                "device_id": "DEV-01",
                "channel": "pos",
                "auth_result": "approved",
            },
            {
                "transaction_id": "tx_travel_manaus_02",
                "customer_id": "C_TRAVEL_01",
                "card_id": "4024007777777777",
                "timestamp": base_time + timedelta(minutes=12),
                "amount": 4200.00,
                "currency": "BRL",
                "merchant_id": "M_MAO_02",
                "merchant_category": "5944",
                "merchant_city": "Manaus",
                "merchant_country": "BR",
                "terminal_id": "T_MAO_02",
                "entry_mode": "chip",
                "device_id": "DEV-01",
                "channel": "pos",
                "auth_result": "approved",
            },
        ]

        import pandas as pd

        df = spark.createDataFrame(pd.DataFrame(travel_txs), schema=TRANSACTION_SCHEMA)
        res = pipeline.process_batch(df)

        pdf = res.enriched.toPandas()
        manaus_tx_res = pdf[pdf["transaction_id"] == "tx_travel_manaus_02"].iloc[0]

        # A transação em Manaus deve ser BLOQUEADA por Impossible Travel
        assert manaus_tx_res["risk_level"] == "block"
        assert manaus_tx_res["risk_score"] >= 70
        assert "impossible_travel" in manaus_tx_res["triggered_rules"]

    def test_e2e_normal_bakery_transaction_approved(self, spark: SparkSession, test_config: Config):
        pipeline = FraudPipeline(spark, test_config, write=False)
        morning_time = datetime(2026, 3, 10, 8, 30, 0, tzinfo=timezone.utc)

        # Transação normal de padaria às 08h30 da manhã
        bakery_tx = [
            {
                "transaction_id": "tx_bakery_01",
                "customer_id": "C_NORMAL_01",
                "card_id": "4532015555555555",
                "timestamp": morning_time,
                "amount": 32.50,
                "currency": "BRL",
                "merchant_id": "M_PADARIA_01",
                "merchant_category": "5814",
                "merchant_city": "São Paulo",
                "merchant_country": "BR",
                "terminal_id": "T_PADARIA_01",
                "entry_mode": "contactless",
                "device_id": "DEV-NORM-01",
                "channel": "pos",
                "auth_result": "approved",
            }
        ]

        import pandas as pd

        df = spark.createDataFrame(pd.DataFrame(bakery_tx), schema=TRANSACTION_SCHEMA)
        res = pipeline.process_batch(df)

        pdf = res.enriched.toPandas()
        bakery_res = pdf[pdf["transaction_id"] == "tx_bakery_01"].iloc[0]

        # Deve ser APROVADA com score baixo (< 30) e nenhuma regra crítica disparada
        assert bakery_res["risk_level"] == "approve"
        assert bakery_res["risk_score"] < 30
        assert len(bakery_res["triggered_rules"]) == 0

    def test_e2e_false_positive_rate_on_normal_transactions(
        self, spark: SparkSession, test_config: Config, tmp_data_dir
    ):
        # Gera 1000 transações sintéticas sem fraudes para validar FPR
        rows, summary = generate(
            spark=spark,
            n_transactions=1000,
            n_cloning=0,
            n_travel=0,
            n_bin=0,
            n_dormant=0,
            n_new_device=0,
            n_unusual=0,
            output_dir=str(tmp_data_dir),
            write=False,
        )

        import pandas as pd

        df = spark.createDataFrame(pd.DataFrame(rows), schema=TRANSACTION_SCHEMA)
        pipeline = FraudPipeline(spark, test_config, write=False)
        res = pipeline.process_batch(df)

        pdf = res.enriched.toPandas()
        total_txs = len(pdf)
        blocked_or_reviewed = len(pdf[pdf["risk_level"].isin(["review", "block"])])
        fpr = (blocked_or_reviewed / total_txs) * 100.0

        print(f"Taxa de falso positivo em transações normais: {fpr:.2f}% ({blocked_or_reviewed}/{total_txs})")
        # Critério: Falso positivo abaixo de 2%
        assert fpr < 2.0
