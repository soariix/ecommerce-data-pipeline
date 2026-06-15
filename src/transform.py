import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

logger = logging.getLogger(__name__)


def cast_dates(df: DataFrame, date_cols: list, fmt: str = "yyyy-MM-dd") -> DataFrame:
    """
    Converte colunas StringType para DateType com formato explícito.
    Usa try_to_date para retornar NULL em vez de lançar exceção em
    datas inválidas (comportamento seguro para pipelines de dados).
    """
    for col in date_cols:
        df = df.withColumn(col, F.try_to_date(F.col(col), fmt))
    return df


def normalize_status(df: DataFrame, col: str = "status") -> DataFrame:
    """Padroniza a coluna de status: lowercase + strip de espaços."""
    return df.withColumn(col, F.lower(F.trim(F.col(col))))


def remove_duplicates(df: DataFrame, key_cols: list) -> DataFrame:
    """Remove duplicatas pela chave informada e loga a quantidade removida."""
    before = df.count()
    df_dedup = df.dropDuplicates(key_cols)
    after = df_dedup.count()
    removed = before - after
    if removed > 0:
        logger.warning(
            "Removidas %d duplicata(s) pela chave %s.", removed, key_cols
        )
    else:
        logger.debug("Sem duplicatas encontradas para a chave %s.", key_cols)
    return df_dedup


def add_order_total(df: DataFrame) -> DataFrame:
    """Calcula o valor total do pedido: quantity × price, arredondado em 2 casas."""
    return df.withColumn(
        "order_total", F.round(F.col("quantity") * F.col("price"), 2)
    )


def enrich_orders(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:

    # Seleciona apenas colunas necessárias de cada lado para evitar ambiguidade
    customers_slim = customers_df.select(
        "customer_id", "city", "state", "signup_date", "last_purchase_date"
    )

    products_slim = (
        products_df
        .select("product_id", "name", "category", "unit_price")
        .withColumnRenamed("name", "product_name")
    )

    enriched = (
        orders_df
        .join(customers_slim, on="customer_id", how="left")
        .join(products_slim,  on="product_id",  how="left")
    )

    logger.info(
        "DataFrame enriquecido: %d linhas × %d colunas.",
        enriched.count(),
        len(enriched.columns),
    )
    return enriched


def build_silver_layer(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    logger.info("Iniciando transformação Silver...")

    orders_clean = (
        orders_df
        .transform(lambda df: cast_dates(df, ["order_date"]))
        .transform(lambda df: normalize_status(df, "status"))
        .transform(lambda df: remove_duplicates(df, ["order_id"]))
        .transform(add_order_total)
    )

    customers_clean = (
        customers_df
        .transform(lambda df: cast_dates(df, ["signup_date", "last_purchase_date"]))
        .transform(lambda df: remove_duplicates(df, ["customer_id"]))
    )

    products_clean = remove_duplicates(products_df, ["product_id"])

    silver_df = enrich_orders(orders_clean, customers_clean, products_clean)

    logger.info("Transformação Silver concluída.")
    return silver_df


def save_parquet(
    df: DataFrame,
    path: str,
    partition_by: list = None,
) -> None:
    """Persiste o DataFrame em formato Parquet (camada Silver)."""
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)
    logger.info("Silver salvo em Parquet: %s", path)


if __name__ == "__main__":
    import os
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s — %(message)s")

    sys.path.insert(0, os.path.dirname(__file__))
    from ingest import create_spark_session, ingest_orders, ingest_customers, ingest_products

    spark = create_spark_session()
    base = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

    orders = ingest_orders(spark,    os.path.join(base, "orders.csv"))
    customers = ingest_customers(spark, os.path.join(base, "customers.csv"))
    products = ingest_products(spark,  os.path.join(base, "products.csv"))

    silver = build_silver_layer(orders, customers, products)

    print("\n=== SILVER LAYER (5 primeiros) ===")
    silver.show(5, truncate=False)
    print("\nSchema Silver:")
    silver.printSchema()
