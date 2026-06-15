import hashlib
import logging
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)

_DEFAULT_SALT = os.getenv("PII_SALT", "ecommerce_pipeline_default_salt")

PII_COLUMNS = ["name", "cpf", "email", "phone"]


def _make_hash_udf(salt: str):
    """Cria uma UDF Spark de hashing SHA-256 com salt fixado no closure."""

    def _hash(value: str) -> str:
        if value is None:
            return None
        digest = hashlib.sha256(f"{salt}|{value}".encode("utf-8")).hexdigest()
        return digest[:16]

    return F.udf(_hash, StringType())


def anonymize_pii(
    df: DataFrame,
    pii_cols: list = None,
    salt: str = _DEFAULT_SALT,
) -> DataFrame:

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
    return (
        df
        .withColumn("pii_anonymized", F.lit(True))
        .withColumn("anonymized_at",  F.current_timestamp())
    )


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
