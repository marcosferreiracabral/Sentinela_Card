"""Fixtures compartilhadas para suíte de testes pytest."""

import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from app import _setup_windows_java
from app.config import Config, load_config

_setup_windows_java()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Instância compartilhada do SparkSession para testes locais rápidos."""
    session = (
        SparkSession.builder.appName("Sentinela_Test_Suite")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def tmp_data_dir() -> Generator[Path, None, None]:
    """Diretório temporário isolado para testes com persistência."""
    d = Path(tempfile.mkdtemp(prefix="sentinela_test_data_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def test_config(tmp_data_dir: Path) -> Config:
    """Configuração padrão carregada do rules.yaml apontando para tmp_data_dir."""
    cfg = load_config("rules.yaml", data_dir=tmp_data_dir)
    return cfg
