"""Testes para persistência Parquet e replay idempotente."""

from datetime import datetime, timezone

from pyspark.sql import SparkSession

from app.audit.replay import verify_replay
from app.config import Config
from app.schemas.transaction import TRANSACTION_SCHEMA
from app.storage.reader import read_enriched, read_raw
from app.streaming.processor import FraudPipeline


class TestStorageAndReplay:
    def test_storage_and_idempotent_replay(self, spark: SparkSession, test_config: Config):
        # 1. Cria transações de teste
        now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
        test_rows = [
            {
                "transaction_id": "tx_test_normal_001",
                "customer_id": "C0001",
                "card_id": "4532011234567890",
                "timestamp": now,
                "amount": 45.0,
                "currency": "BRL",
                "merchant_id": "M00001",
                "merchant_category": "5411",
                "merchant_city": "São Paulo",
                "merchant_country": "BR",
                "terminal_id": "T000001",
                "entry_mode": "chip",
                "device_id": "DEV-001",
                "channel": "pos",
                "auth_result": "approved",
            },
            {
                "transaction_id": "tx_test_fraud_002",
                "customer_id": "C0002",
                "card_id": "4024009876543210",
                "timestamp": now,
                "amount": 9500.0,
                "currency": "BRL",
                "merchant_id": "M00002",
                "merchant_category": "5944",
                "merchant_city": "Manaus",
                "merchant_country": "BR",
                "terminal_id": "T000002",
                "entry_mode": "chip",
                "device_id": "DEV-NEW-99",
                "channel": "pos",
                "auth_result": "approved",
            },
        ]

        import pandas as pd

        batch_df = spark.createDataFrame(pd.DataFrame(test_rows), schema=TRANSACTION_SCHEMA)

        # 2. Executa o pipeline com gravação
        pipeline = FraudPipeline(spark, test_config, write=True)
        _ = pipeline.process_batch(batch_df)

        raw_df = read_raw(test_config, spark)
        enriched_df = read_enriched(test_config, spark)
        assert raw_df is not None
        assert raw_df.count() == 2
        assert enriched_df is not None
        assert enriched_df.count() == 2

        # 3. Teste de reprocessamento idempotente (segundo batch com mesmas IDs não duplica)
        _ = pipeline.process_batch(batch_df)
        raw_df_after = read_raw(test_config, spark)
        enriched_df_after = read_enriched(test_config, spark)
        assert raw_df_after is not None
        assert raw_df_after.count() == 2
        assert enriched_df_after is not None
        assert enriched_df_after.count() == 2

        # 4. Teste de verificação de replay
        verdict_normal = verify_replay(spark, test_config, "tx_test_normal_001")
        assert verdict_normal["idempotent"] is True
        assert verdict_normal["stored_level"] == verdict_normal["recomputed_level"]

        verdict_fraud = verify_replay(spark, test_config, "tx_test_fraud_002")
        assert verdict_fraud["idempotent"] is True
