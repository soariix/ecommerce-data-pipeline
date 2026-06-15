import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Caminhos base dentro do container Airflow
_SRC_PATH = "/opt/airflow/src"
_BASE_RAW = "/opt/airflow/data/raw"
_BASE_OUT = "/opt/airflow/data/output"
_TMP_PATH = f"{_BASE_OUT}/tmp"

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


def _ensure_src_path():
    if _SRC_PATH not in sys.path:
        sys.path.insert(0, _SRC_PATH)


def task_ingest(**context):
    """Lê os arquivos brutos e persiste como Parquet temporário."""
    _ensure_src_path()
    from ingest import create_spark_session, ingest_customers, ingest_orders, ingest_products, ingest_reviews

    spark = create_spark_session("AirflowIngest")
    try:
        orders = ingest_orders(spark,    f"{_BASE_RAW}/orders.csv")
        customers = ingest_customers(spark, f"{_BASE_RAW}/customers.csv")
        products = ingest_products(spark,  f"{_BASE_RAW}/products.csv")
        reviews = ingest_reviews(spark,   f"{_BASE_RAW}/reviews.json")

        orders.write.mode("overwrite").parquet(f"{_TMP_PATH}/orders")
        customers.write.mode("overwrite").parquet(f"{_TMP_PATH}/customers")
        products.write.mode("overwrite").parquet(f"{_TMP_PATH}/products")
        reviews.write.mode("overwrite").parquet(f"{_TMP_PATH}/reviews")

        context["task_instance"].xcom_push(
            key="orders_count", value=orders.count())
    finally:
        spark.stop()


def task_compliance(**context):
    """Aplica anonimização LGPD nos dados de clientes."""
    _ensure_src_path()
    from compliance import anonymize_pii
    from ingest import create_spark_session

    spark = create_spark_session("AirflowCompliance")
    try:
        customers = spark.read.parquet(f"{_TMP_PATH}/customers")
        anonymize_pii(customers).write.mode("overwrite").parquet(
            f"{_TMP_PATH}/customers_anon"
        )
    finally:
        spark.stop()


def task_quality_gate(**context):
    """
    Executa checks de qualidade. Lança exceção se algum check ERROR falhar,
    interrompendo o pipeline com retry automático.
    """
    _ensure_src_path()
    from ingest import create_spark_session
    from quality import run_orders_quality

    spark = create_spark_session("AirflowQuality")
    try:
        orders = spark.read.parquet(f"{_TMP_PATH}/orders")
        customers = spark.read.parquet(f"{_TMP_PATH}/customers")
        products = spark.read.parquet(f"{_TMP_PATH}/products")

        report = run_orders_quality(orders, customers, products)
        if not report.passed:
            failed = [f"[{e.column}] {e.details}" for e in report.errors]
            raise ValueError(f"Quality gate falhou: {failed}")
    finally:
        spark.stop()


def task_transform_silver(**context):
    """Executa o pipeline de transformação e grava a camada Silver."""
    _ensure_src_path()
    from ingest import create_spark_session
    from transform import build_silver_layer, save_parquet

    spark = create_spark_session("AirflowTransform")
    try:
        orders = spark.read.parquet(f"{_TMP_PATH}/orders")
        customers_anon = spark.read.parquet(f"{_TMP_PATH}/customers_anon")
        products = spark.read.parquet(f"{_TMP_PATH}/products")

        silver = build_silver_layer(orders, customers_anon, products)
        save_parquet(silver, f"{_BASE_OUT}/silver", partition_by=["status"])
    finally:
        spark.stop()


def task_analytics_gold(**context):
    """Gera as tabelas Gold e persiste em Parquet."""
    _ensure_src_path()
    from analytics import run_all_analytics, save_gold_parquet
    from ingest import create_spark_session

    spark = create_spark_session("AirflowAnalytics")
    try:
        silver = spark.read.parquet(f"{_BASE_OUT}/silver")
        gold = run_all_analytics(spark, silver)
        save_gold_parquet(gold, f"{_BASE_OUT}/gold")
    finally:
        spark.stop()


def task_load_postgres(**context):
    """Carrega tabelas Gold no PostgreSQL via JDBC."""
    _ensure_src_path()
    from analytics import load_to_postgres
    from ingest import create_spark_session

    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_db = os.getenv("POSTGRES_DB",   "ecommerce_gold")
    user = os.getenv("POSTGRES_USER",  "pipeline")
    password = os.getenv("POSTGRES_PASSWORD", "pipeline123")
    jdbc_url = f"jdbc:postgresql://{postgres_host}:5432/{postgres_db}"

    spark = create_spark_session("AirflowLoadPostgres")
    try:
        for table in ["revenue_by_category", "top_customers", "churn_risk_score"]:
            df = spark.read.parquet(f"{_BASE_OUT}/gold/{table}")
            load_to_postgres(df, table, jdbc_url, user, password)
    finally:
        spark.stop()


with DAG(
    dag_id="ecommerce_data_pipeline",
    default_args=default_args,
    description="Pipeline de dados de e-commerce: Raw → Silver → Gold → PostgreSQL",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "data-pipeline", "pyspark", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=task_ingest,
    )

    compliance = PythonOperator(
        task_id="compliance_anonymization",
        python_callable=task_compliance,
    )

    quality = PythonOperator(
        task_id="quality_gate",
        python_callable=task_quality_gate,
    )

    transform = PythonOperator(
        task_id="transform_silver",
        python_callable=task_transform_silver,
    )

    analytics = PythonOperator(
        task_id="analytics_gold",
        python_callable=task_analytics_gold,
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=task_load_postgres,
    )

    start >> ingest >> compliance >> quality >> transform >> analytics >> load_postgres >> end
