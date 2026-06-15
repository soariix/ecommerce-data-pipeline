"""
compliance.py — Conformidade com LGPD / GDPR

Anonimiza campos PII (Personally Identifiable Information) antes de qualquer
processamento ou persistência de dados.

Técnica: Hash SHA-256 com salt externo configurável via variável de ambiente.
O resultado é irreversível sem o salt — em conformidade com LGPD Art. 12,
que trata dados anonimizados como dados não pessoais.

Decisão técnica:
  - Salt via env var permite key rotation sem re-processar dados históricos.
  - Hash truncado em 16 chars preserva unicidade para joins internos sem
    expor o hash completo.
  - NUNCA persistir o salt junto aos dados anonimizados.
"""
import hashlib
import logging
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)

# Salt lido de variável de ambiente — nunca hardcode em produção
_DEFAULT_SALT = os.getenv("PII_SALT", "ecommerce_pipeline_default_salt")

# Colunas PII padrão da tabela de clientes
PII_COLUMNS = ["name", "cpf", "email", "phone"]


# ── UDF de hashing ────────────────────────────────────────────────────────────

def _make_hash_udf(salt: str):
    """Cria uma UDF Spark de hashing SHA-256 com salt fixado no closure."""

    def _hash(value: str) -> str:
        if value is None:
            return None
        digest = hashlib.sha256(f"{salt}|{value}".encode("utf-8")).hexdigest()
        return digest[:16]

    return F.udf(_hash, StringType())


# ── Funções públicas ──────────────────────────────────────────────────────────

def anonymize_pii(
    df: DataFrame,
    pii_cols: list = None,
    salt: str = _DEFAULT_SALT,
) -> DataFrame:
    """
    Substitui colunas PII por hash SHA-256 truncado (16 chars).

    Args:
        df:       DataFrame com dados de clientes.
        pii_cols: Lista de colunas a anonimizar. Padrão: PII_COLUMNS.
        salt:     Salt para o hash. Padrão: variável de ambiente PII_SALT.

    Returns:
        DataFrame com colunas PII substituídas por hash.
    """
    cols_to_hash = pii_cols or PII_COLUMNS
    hash_udf = _make_hash_udf(salt)
    existing = set(df.columns)

    for col in cols_to_hash:
        if col in existing:
            df = df.withColumn(col, hash_udf(F.col(col)))
            logger.debug("Coluna anonimizada: %s", col)
        else:
            logger.warning("Coluna PII não encontrada no DataFrame: %s", col)

    logger.info(
        "Anonimização LGPD aplicada. Colunas: %s",
        [c for c in cols_to_hash if c in existing],
    )
    return df


def add_compliance_metadata(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas de auditoria de compliance para rastreabilidade.

    Colunas adicionadas:
      - pii_anonymized : flag booleano
      - anonymized_at  : timestamp da anonimização
    """
    return (
        df
        .withColumn("pii_anonymized", F.lit(True))
        .withColumn("anonymized_at",  F.current_timestamp())
    )


# ── Execução direta (debug) ───────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s — %(message)s")

    sys.path.insert(0, os.path.dirname(__file__))
    from ingest import create_spark_session, ingest_customers

    spark = create_spark_session()
    path = os.path.join(os.path.dirname(__file__), "..",
                        "data", "raw", "customers.csv")
    customers = ingest_customers(spark, path)

    print("ANTES da anonimização:")
    customers.select("customer_id", "name", "cpf",
                     "email").show(3, truncate=False)

    anon = anonymize_pii(customers)
    print("DEPOIS da anonimização (SHA-256 + salt):")
    anon.select("customer_id", "name", "cpf", "email").show(3, truncate=False)
