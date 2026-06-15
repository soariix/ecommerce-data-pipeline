"""
quality.py — Validações de qualidade de dados

Implementa checks declarativos que produzem um QualityReport estruturado:
  - check_nulls               → colunas obrigatórias sem nulos
  - check_duplicates          → unicidade de chave primária/composta
  - check_range               → valores numéricos dentro do range esperado
  - check_allowed_values      → valores pertencem a conjunto permitido
  - check_referential_integrity → chaves estrangeiras resolvidas

O pipeline é interrompido se qualquer check de severidade ERROR falhar.
"""
import logging
from dataclasses import dataclass, field
from typing import Any, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


# ── Modelos de resultado ──────────────────────────────────────────────────────

@dataclass
class CheckResult:
    check_name: str
    column: Optional[str]
    passed: bool
    details: str
    severity: str = "ERROR"   # ERROR | WARNING


@dataclass
class QualityReport:
    table_name: str
    total_rows: int
    checks: List[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks if c.severity == "ERROR")

    @property
    def errors(self) -> List[CheckResult]:
        return [c for c in self.checks if not c.passed and c.severity == "ERROR"]

    @property
    def warnings(self) -> List[CheckResult]:
        return [c for c in self.checks if not c.passed and c.severity == "WARNING"]

    def summary(self) -> str:
        status = "PASSOU" if self.passed else "FALHOU"
        lines = [
            "",
            "=" * 60,
            f"Tabela : {self.table_name}  [{status}]",
            f"Linhas : {self.total_rows}",
            f"Checks : {len(self.checks)} executados",
            f"Erros  : {len(self.errors)}  |  Avisos: {len(self.warnings)}",
        ]
        for c in self.checks:
            icon = "OK" if c.passed else (
                "ERR" if c.severity == "ERROR" else "WRN")
            lines.append(f"  [{icon}] {c.check_name}({c.column}): {c.details}")
        lines.append("=" * 60)
        return "\n".join(lines)


# ── Checks individuais ────────────────────────────────────────────────────────

def check_nulls(
    df: DataFrame,
    columns: List[str],
    severity: str = "ERROR",
) -> List[CheckResult]:
    """Verifica colunas obrigatórias por valores nulos."""
    null_counts = (
        df.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in columns
        ])
        .collect()[0]
        .asDict()
    )

    results = []
    for col, count in null_counts.items():
        passed = count == 0
        results.append(CheckResult(
            check_name="null_check",
            column=col,
            passed=passed,
            details=f"{count} valores nulos encontrados" if not passed else "OK",
            severity=severity,
        ))
    return results


def check_duplicates(
    df: DataFrame,
    key_cols: List[str],
    severity: str = "ERROR",
) -> CheckResult:
    """Verifica unicidade de chave primária ou composta."""
    total = df.count()
    distinct = df.dropDuplicates(key_cols).count()
    dup_count = total - distinct
    return CheckResult(
        check_name="duplicate_check",
        column=str(key_cols),
        passed=dup_count == 0,
        details=f"{dup_count} duplicata(s) encontrada(s)" if dup_count else "OK",
        severity=severity,
    )


def check_range(
    df: DataFrame,
    column: str,
    min_val: Any = None,
    max_val: Any = None,
    severity: str = "ERROR",
) -> CheckResult:
    """Verifica se valores numéricos estão dentro do range [min_val, max_val]."""
    condition = F.lit(False)
    if min_val is not None:
        condition = condition | (F.col(column) < min_val)
    if max_val is not None:
        condition = condition | (F.col(column) > max_val)

    out_of_range = df.filter(condition).count()
    return CheckResult(
        check_name="range_check",
        column=column,
        passed=out_of_range == 0,
        details=(
            f"{out_of_range} valor(es) fora do range [{min_val}, {max_val}]"
            if out_of_range else "OK"
        ),
        severity=severity,
    )


def check_allowed_values(
    df: DataFrame,
    column: str,
    allowed: List[Any],
    severity: str = "ERROR",
) -> CheckResult:
    """Verifica se todos os valores de uma coluna pertencem ao conjunto permitido."""
    invalid = df.filter(~F.col(column).isin(allowed)).count()
    return CheckResult(
        check_name="allowed_values_check",
        column=column,
        passed=invalid == 0,
        details=f"{invalid} valor(es) inválido(s) em '{column}'" if invalid else "OK",
        severity=severity,
    )


def check_referential_integrity(
    df_child: DataFrame,
    df_parent: DataFrame,
    child_key: str,
    parent_key: str,
    severity: str = "ERROR",
) -> CheckResult:
    """Verifica integridade referencial entre tabela filha e tabela pai."""
    orphans = (
        df_child
        .join(
            df_parent.select(parent_key),
            df_child[child_key] == df_parent[parent_key],
            "left_anti",
        )
        .count()
    )
    return CheckResult(
        check_name="referential_integrity_check",
        column=f"{child_key} -> {parent_key}",
        passed=orphans == 0,
        details=f"{orphans} registro(s) órfão(s) encontrado(s)" if orphans else "OK",
        severity=severity,
    )


# ── Suite de qualidade da tabela Orders ──────────────────────────────────────

def run_orders_quality(
    df: DataFrame,
    customers_df: DataFrame = None,
    products_df: DataFrame = None,
) -> QualityReport:
    """
    Executa a suite completa de qualidade na tabela de pedidos.

    Checks de severidade ERROR (interrompem o pipeline se falharem):
      - Nulos em colunas-chave (order_id, customer_id, product_id, price)
      - Duplicatas por order_id
      - Price > 0
      - Quantidade no range [1, 9999]
      - Status dentro do conjunto de valores permitidos
      - Integridade referencial com customers e products (se fornecidos)

    Checks de severidade WARNING (logados mas não interrompem):
      - Nulos em colunas opcionais (quantity, status, payment_method)
    """
    report = QualityReport(table_name="orders", total_rows=df.count())

    # Nulos críticos
    report.checks.extend(
        check_nulls(df, ["order_id", "customer_id", "product_id", "price"])
    )

    # Nulos opcionais
    report.checks.extend(
        check_nulls(df, ["quantity", "status",
                    "payment_method"], severity="WARNING")
    )

    # Unicidade
    report.checks.append(check_duplicates(df, ["order_id"]))

    # Ranges
    report.checks.append(check_range(df, "price",    min_val=0.01))
    report.checks.append(check_range(df, "quantity", min_val=1, max_val=9_999))

    # Valores permitidos de status
    valid_statuses = ["completed", "cancelled",
                      "processing", "refunded", "pending"]
    report.checks.append(check_allowed_values(df, "status", valid_statuses))

    # Integridade referencial
    if customers_df is not None:
        report.checks.append(
            check_referential_integrity(
                df, customers_df, "customer_id", "customer_id")
        )
    if products_df is not None:
        report.checks.append(
            check_referential_integrity(
                df, products_df, "product_id", "product_id")
        )

    # Log detalhado de cada check
    for check in report.checks:
        if not check.passed and check.severity == "ERROR":
            logger.error("[%s] %s: %s", check.check_name,
                         check.column, check.details)
        elif not check.passed and check.severity == "WARNING":
            logger.warning("[%s] %s: %s", check.check_name,
                           check.column, check.details)
        else:
            logger.info("[%s] %s: %s", check.check_name,
                        check.column, check.details)

    print(report.summary())
    return report


# ── Execução direta (debug) ───────────────────────────────────────────────────

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

    run_orders_quality(orders, customers, products)
