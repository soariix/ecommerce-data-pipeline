"""
ingest.py — Camada de ingestão de dados brutos (Raw Layer)

Responsável por ler arquivos CSV/JSON com schema explícito via PySpark.
Schema explícito evita inferência automática (cara em produção) e garante
que tipos incorretos nos dados brutos sejam detectados imediatamente.
"""
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

# ── Schemas explícitos ────────────────────────────────────────────────────────

ORDERS_SCHEMA = StructType([
    StructField("order_id",       IntegerType(), nullable=False),
    StructField("customer_id",    IntegerType(), nullable=False),
    StructField("product_id",     IntegerType(), nullable=False),
    StructField("quantity",       IntegerType(), nullable=True),
    StructField("price",          DoubleType(),  nullable=True),
    StructField("status",         StringType(),  nullable=True),
    StructField("order_date",     StringType(),  nullable=True),
    StructField("payment_method", StringType(),  nullable=True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",        IntegerType(), nullable=False),
    StructField("name",               StringType(),  nullable=True),
    StructField("cpf",                StringType(),  nullable=True),
    StructField("email",              StringType(),  nullable=True),
    StructField("phone",              StringType(),  nullable=True),
    StructField("city",               StringType(),  nullable=True),
    StructField("state",              StringType(),  nullable=True),
    StructField("signup_date",        StringType(),  nullable=True),
    StructField("last_purchase_date", StringType(),  nullable=True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id",  IntegerType(), nullable=False),
    StructField("name",        StringType(),  nullable=True),
    StructField("category",    StringType(),  nullable=True),
    StructField("unit_price",  DoubleType(),  nullable=True),
    StructField("stock_qty",   IntegerType(), nullable=True),
    StructField("supplier_id", StringType(),  nullable=True),
])


# ── SparkSession factory ──────────────────────────────────────────────────────

def create_spark_session(app_name: str = "EcommerceDataPipeline") -> SparkSession:
    """Cria e retorna uma SparkSession configurada para execução local."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession iniciada: %s", app_name)
    return spark


# ── Funções de ingestão ───────────────────────────────────────────────────────

def ingest_orders(spark: SparkSession, path: str) -> DataFrame:
    """Lê o arquivo CSV de pedidos com schema explícito."""
    logger.info("Ingerindo pedidos de: %s", path)
    df = spark.read.csv(path, header=True,
                        schema=ORDERS_SCHEMA, mode="PERMISSIVE")
    count = df.count()
    logger.info("  → %d registros carregados (orders).", count)
    return df


def ingest_customers(spark: SparkSession, path: str) -> DataFrame:
    """Lê o arquivo CSV de clientes com schema explícito."""
    logger.info("Ingerindo clientes de: %s", path)
    df = spark.read.csv(path, header=True,
                        schema=CUSTOMERS_SCHEMA, mode="PERMISSIVE")
    count = df.count()
    logger.info("  → %d registros carregados (customers).", count)
    return df


def ingest_products(spark: SparkSession, path: str) -> DataFrame:
    """Lê o arquivo CSV de produtos com schema explícito."""
    logger.info("Ingerindo produtos de: %s", path)
    df = spark.read.csv(path, header=True,
                        schema=PRODUCTS_SCHEMA, mode="PERMISSIVE")
    count = df.count()
    logger.info("  → %d registros carregados (products).", count)
    return df


def ingest_reviews(spark: SparkSession, path: str) -> DataFrame:
    """
    Lê avaliações de produtos no formato JSON multiline.
    Simula leitura de uma coleção MongoDB exportada para JSON.
    """
    logger.info("Ingerindo reviews de: %s", path)
    df = spark.read.json(path, multiLine=True)
    count = df.count()
    logger.info("  → %d registros carregados (reviews).", count)
    return df


# ── Execução direta (debug) ───────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s — %(message)s")
    spark = create_spark_session()

    base = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

    orders = ingest_orders(spark,    os.path.join(base, "orders.csv"))
    customers = ingest_customers(spark, os.path.join(base, "customers.csv"))
    products = ingest_products(spark,  os.path.join(base, "products.csv"))
    reviews = ingest_reviews(spark,   os.path.join(base, "reviews.json"))

    print("\n=== ORDERS (5 primeiros) ===")
    orders.show(5, truncate=False)
    print("\n=== CUSTOMERS (5 primeiros) ===")
    customers.show(5, truncate=False)
    print("\n=== PRODUCTS ===")
    products.show(truncate=False)
    print("\n=== REVIEWS (5 primeiros) ===")
    reviews.show(5, truncate=False)
