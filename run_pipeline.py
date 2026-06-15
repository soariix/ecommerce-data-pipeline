#!/usr/bin/env python3

import logging
import os
import sys

# ── Garante JAVA_HOME e HADOOP_HOME independente do terminal ──────────────────
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

# Adiciona src/ ao Python path — deve vir ANTES dos imports locais
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "src"))

from analytics import run_all_analytics, save_gold_parquet  # noqa: E402
from compliance import anonymize_pii  # noqa: E402
from ingest import (  # noqa: E402
    create_spark_session,
    ingest_customers,
    ingest_orders,
    ingest_products,
    ingest_reviews,
)
from quality import run_orders_quality  # noqa: E402
from transform import build_silver_layer, save_parquet  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline")

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_RAW = os.path.join(_BASE_DIR, "data", "raw")
_BASE_OUT = os.path.join(_BASE_DIR, "data", "output")
_SILVER_PATH = os.path.join(_BASE_OUT, "silver")
_GOLD_PATH = os.path.join(_BASE_OUT, "gold")


def main() -> None:
    sep = "=" * 60
    logger.info(sep)
    logger.info("  ECOMMERCE DATA PIPELINE — INICIANDO")
    logger.info(sep)

    spark = create_spark_session("EcommerceDataPipeline")

    logger.info("[1/5] Ingestao de dados brutos...")
    orders = ingest_orders(spark,    os.path.join(_BASE_RAW, "orders.csv"))
    customers = ingest_customers(
        spark, os.path.join(_BASE_RAW, "customers.csv"))
    products = ingest_products(spark,  os.path.join(_BASE_RAW, "products.csv"))
    reviews = ingest_reviews(spark,   os.path.join(_BASE_RAW, "reviews.json"))

    logger.info(
        "Ingestao concluida: %d pedidos | %d clientes | %d produtos | %d reviews",
        orders.count(), customers.count(), products.count(), reviews.count(),
    )

    logger.info("[2/5] Anonimizando PII (LGPD Art. 12)...")
    customers_anon = anonymize_pii(customers)

    logger.info("[3/5] Executando quality checks...")
    report = run_orders_quality(orders, customers, products)

    if not report.passed:
        logger.error("Pipeline interrompido — quality gate FALHOU.")
        for err in report.errors:
            logger.error("  -> [%s] %s", err.column, err.details)
        sys.exit(1)

    logger.info("Quality gate PASSOU. Seguindo para transformacao...")

    logger.info("[4/5] Construindo camada Silver...")
    silver_df = build_silver_layer(orders, customers_anon, products)
    save_parquet(silver_df, _SILVER_PATH, partition_by=["status"])

    logger.info("[5/5] Gerando tabelas Gold (Spark SQL)...")
    logger.info("[5/5] Gerando tabelas Gold (Spark SQL)...")
    gold = run_all_analytics(spark, silver_df)
    save_gold_parquet(gold, _GOLD_PATH)

    print(f"\n{sep}")
    print("  RESULTADOS — CAMADA GOLD")
    print(sep)

    print("\nRevenue by Category:")
    gold["revenue_by_category"].show(truncate=False)

    print("Top 10 Customers (Lifetime Value):")
    gold["top_customers"].show(truncate=False)

    print("Churn Risk Score:")
    gold["churn_risk_score"].show(truncate=False)

    logger.info("Pipeline concluido com sucesso!")
    logger.info("Outputs em: %s", _BASE_OUT)


if __name__ == "__main__":
    main()
