import os
import sys

import pytest

# ── Garante JAVA_HOME e HADOOP_HOME independente do terminal ──────────────────
# Necessário no Windows: PySpark não encontra o Java gateway sem essas vars.
_JAVA_HOME = r"C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot"
_HADOOP_HOME = r"C:\hadoop"

if not os.environ.get("JAVA_HOME") and os.path.isdir(_JAVA_HOME):
    os.environ["JAVA_HOME"] = _JAVA_HOME
    os.environ["PATH"] = os.path.join(
        _JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

if not os.environ.get("HADOOP_HOME") and os.path.isdir(_HADOOP_HOME):
    os.environ["HADOOP_HOME"] = _HADOOP_HOME
    os.environ["PATH"] = os.path.join(
        _HADOOP_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession  # noqa: E402  (deve vir após setar env vars)

# Adiciona o diretório raiz ao Python path para importar módulos em src.*
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="session")
def spark():
    """SparkSession local compartilhada por toda a suíte de testes."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("EcommerceDataPipelineTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
