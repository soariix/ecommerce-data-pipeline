"""
analytics.py — Camada Gold: métricas analíticas via Spark SQL

Produz três tabelas analíticas prontas para consumo:
  - revenue_by_category  → receita, ticket médio e share por categoria
  - top_customers        → ranking de clientes por lifetime value
  - churn_risk_score     → score de risco de churn por cliente
"""
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


# ── Helper interno ────────────────────────────────────────────────────────────

def _register_view(df: DataFrame, view_name: str) -> None:
    df.createOrReplaceTempView(view_name)
    logger.debug("Temp view registrada: %s", view_name)


# ── Queries Gold ──────────────────────────────────────────────────────────────

def revenue_by_category(spark: SparkSession, silver_df: DataFrame) -> DataFrame:
    """
    Gold: receita total, ticket médio e participação percentual por categoria.
    Considera apenas pedidos com status 'completed'.
    """
    _register_view(silver_df, "silver_orders")
    return spark.sql("""
        SELECT
            category,
            COUNT(DISTINCT order_id)                                AS total_orders,
            ROUND(SUM(order_total), 2)                              AS total_revenue,
            ROUND(AVG(order_total), 2)                              AS avg_ticket,
            ROUND(
                SUM(order_total) * 100.0
                / SUM(SUM(order_total)) OVER ()
            , 2)                                                    AS revenue_share_pct
        FROM silver_orders
        WHERE status = 'completed'
        GROUP BY category
        ORDER BY total_revenue DESC
    """)


def top_customers(
    spark: SparkSession,
    silver_df: DataFrame,
    top_n: int = 10,
) -> DataFrame:
    """
    Gold: ranking dos N clientes por receita gerada (lifetime value).
    Inclui ticket médio, total de pedidos e datas de primeira/última compra.
    """
    _register_view(silver_df, "silver_orders")
    return spark.sql(f"""
        SELECT
            customer_id,
            COUNT(DISTINCT order_id)    AS total_orders,
            ROUND(SUM(order_total), 2)  AS lifetime_value,
            ROUND(AVG(order_total), 2)  AS avg_ticket,
            MAX(order_date)             AS last_order_date,
            MIN(order_date)             AS first_order_date
        FROM silver_orders
        WHERE status = 'completed'
        GROUP BY customer_id
        ORDER BY lifetime_value DESC
        LIMIT {top_n}
    """)


def churn_risk_score(spark: SparkSession, silver_df: DataFrame) -> DataFrame:
    """
    Gold: score de risco de churn por cliente baseado em recência.

    Regra de negócio:
      - days_since_last_order > 60  → HIGH   (cliente dormindo)
      - days_since_last_order 31-60 → MEDIUM (atenção)
      - days_since_last_order <= 30 → LOW    (cliente ativo)
    """
    _register_view(silver_df, "silver_orders")
    return spark.sql("""
        WITH customer_activity AS (
            SELECT
                customer_id,
                MAX(order_date)                                     AS last_order_date,
                COUNT(DISTINCT order_id)                            AS total_orders,
                ROUND(SUM(order_total), 2)                          AS lifetime_value,
                DATEDIFF(CURRENT_DATE(), MAX(order_date))           AS days_since_last_order
            FROM silver_orders
            WHERE status = 'completed'
            GROUP BY customer_id
        )
        SELECT
            customer_id,
            last_order_date,
            total_orders,
            lifetime_value,
            days_since_last_order,
            CASE
                WHEN days_since_last_order > 60 THEN 'HIGH'
                WHEN days_since_last_order > 30 THEN 'MEDIUM'
                ELSE 'LOW'
            END                                                     AS churn_risk
        FROM customer_activity
        ORDER BY days_since_last_order DESC
    """)


# ── Orquestração ──────────────────────────────────────────────────────────────

def run_all_analytics(spark: SparkSession, silver_df: DataFrame) -> dict:
    """
    Executa todas as queries Gold e retorna um dicionário de DataFrames.

    Returns:
        {
            "revenue_by_category": DataFrame,
            "top_customers":       DataFrame,
            "churn_risk_score":    DataFrame,
        }
    """
    logger.info("Gerando tabelas Gold...")
    gold = {
        "revenue_by_category": revenue_by_category(spark, silver_df),
        "top_customers":       top_customers(spark, silver_df),
        "churn_risk_score":    churn_risk_score(spark, silver_df),
    }
    for name, df in gold.items():
        logger.info("Gold [%s]: %d linhas.", name, df.count())
    return gold


# ── Persistência ──────────────────────────────────────────────────────────────

def save_gold_parquet(gold: dict, base_path: str) -> None:
    """Salva todas as tabelas Gold em Parquet."""
    for name, df in gold.items():
        path = os.path.join(base_path, name)
        df.write.mode("overwrite").parquet(path)
        logger.info("Gold [%s] salvo em: %s", name, path)


def load_to_postgres(
    df: DataFrame,
    table: str,
    jdbc_url: str,
    user: str,
    password: str,
) -> None:
    """
    Persiste DataFrame na camada Gold do PostgreSQL via JDBC.

    Requer o driver PostgreSQL JDBC no classpath do Spark.
    Configure via: spark.jars.packages=org.postgresql:postgresql:42.7.3
    """
    (
        df.write
        .format("jdbc")
        .option("url",      jdbc_url)
        .option("dbtable",  f"gold.{table}")
        .option("user",     user)
        .option("password", password)
        .option("driver",   "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    logger.info("Gold [%s] carregado no PostgreSQL.", table)


# ── Execução direta (debug) ───────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s — %(message)s")

    sys.path.insert(0, os.path.dirname(__file__))
    from ingest import create_spark_session, ingest_orders, ingest_customers, ingest_products
    from transform import build_silver_layer
    from compliance import anonymize_pii

    spark = create_spark_session()
    base_raw = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

    orders = ingest_orders(spark,    os.path.join(base_raw, "orders.csv"))
    customers = ingest_customers(
        spark, os.path.join(base_raw, "customers.csv"))
    products = ingest_products(spark,  os.path.join(base_raw, "products.csv"))

    customers_anon = anonymize_pii(customers)
    silver = build_silver_layer(orders, customers_anon, products)
    gold = run_all_analytics(spark, silver)

    print("\n=== Revenue by Category ===")
    gold["revenue_by_category"].show(truncate=False)

    print("\n=== Top 10 Customers ===")
    gold["top_customers"].show(truncate=False)

    print("\n=== Churn Risk Score ===")
    gold["churn_risk_score"].show(truncate=False)
