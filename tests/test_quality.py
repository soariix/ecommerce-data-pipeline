from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.quality import (
    check_allowed_values,
    check_duplicates,
    check_nulls,
    check_range,
    check_referential_integrity,
)


class TestCheckNulls:
    def test_no_nulls_passes(self, spark):
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        results = check_nulls(df, ["id", "name"])
        assert all(r.passed for r in results)

    def test_detects_single_null(self, spark):
        df = spark.createDataFrame([(1, "Alice"), (2, None)], ["id", "name"])
        results = check_nulls(df, ["name"])
        assert not results[0].passed
        assert "1 valor" in results[0].details

    def test_detects_multiple_nulls(self, spark):
        df = spark.createDataFrame(
            [(1, None, "a@b.com"), (2, "Bob", None)],
            ["id", "name", "email"],
        )
        results = check_nulls(df, ["id", "name", "email"])
        by_col = {r.column: r.passed for r in results}
        assert by_col["id"] is True
        assert by_col["name"] is False
        assert by_col["email"] is False

    def test_warning_severity_does_not_fail_overall(self, spark):
        schema = StructType([
            StructField("id",           IntegerType(), True),
            StructField("optional_col", StringType(),  True),
        ])
        df = spark.createDataFrame([(1, None)], schema)
        results = check_nulls(df, ["optional_col"], severity="WARNING")
        assert not results[0].passed
        assert results[0].severity == "WARNING"

    def test_empty_dataframe_passes(self, spark):
        schema = StructType([StructField("id", IntegerType(), True)])
        df = spark.createDataFrame([], schema)
        results = check_nulls(df, ["id"])
        assert all(r.passed for r in results)


class TestCheckDuplicates:
    def test_no_duplicates_passes(self, spark):
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        result = check_duplicates(df, ["id"])
        assert result.passed

    def test_detects_one_duplicate(self, spark):
        df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
        result = check_duplicates(df, ["id"])
        assert not result.passed
        assert "1 duplicata" in result.details

    def test_detects_multiple_duplicates(self, spark):
        df = spark.createDataFrame([(1,), (1,), (2,), (2,), (2,)], ["id"])
        result = check_duplicates(df, ["id"])
        assert not result.passed

    def test_composite_key_no_duplicates(self, spark):
        df = spark.createDataFrame(
            [(1, 10), (1, 20), (2, 10)],
            ["order_id", "product_id"],
        )
        result = check_duplicates(df, ["order_id", "product_id"])
        assert result.passed

    def test_composite_key_with_duplicate(self, spark):
        df = spark.createDataFrame(
            [(1, 10), (1, 10), (2, 10)],
            ["order_id", "product_id"],
        )
        result = check_duplicates(df, ["order_id", "product_id"])
        assert not result.passed


class TestCheckRange:
    def test_all_in_range_passes(self, spark):
        df = spark.createDataFrame([(10.0,), (50.0,), (99.99,)], ["price"])
        result = check_range(df, "price", min_val=0.01, max_val=999.99)
        assert result.passed

    def test_detects_negative_price(self, spark):
        df = spark.createDataFrame([(10.0,), (-5.0,), (100.0,)], ["price"])
        result = check_range(df, "price", min_val=0.01)
        assert not result.passed
        assert "1 valor" in result.details

    def test_detects_above_max(self, spark):
        df = spark.createDataFrame([(1,), (5,), (101,)], ["qty"])
        result = check_range(df, "qty", max_val=100)
        assert not result.passed

    def test_only_min_bound(self, spark):
        df = spark.createDataFrame([(0.0,), (1.0,)], ["price"])
        result = check_range(df, "price", min_val=0.01)
        assert not result.passed  # 0.0 viola min_val

    def test_only_max_bound(self, spark):
        df = spark.createDataFrame([(5,), (10,)], ["qty"])
        result = check_range(df, "qty", max_val=10)
        assert result.passed

    def test_exact_boundary_values_pass(self, spark):
        df = spark.createDataFrame([(1,), (9999,)], ["qty"])
        result = check_range(df, "qty", min_val=1, max_val=9999)
        assert result.passed


class TestCheckAllowedValues:
    def test_all_valid_passes(self, spark):
        df = spark.createDataFrame(
            [("completed",), ("cancelled",)], ["status"])
        allowed = ["completed", "cancelled", "processing", "refunded"]
        result = check_allowed_values(df, "status", allowed)
        assert result.passed

    def test_detects_invalid_value(self, spark):
        df = spark.createDataFrame(
            [("completed",), ("unknown_xyz",)], ["status"])
        result = check_allowed_values(df, "status", ["completed", "cancelled"])
        assert not result.passed
        assert "1 valor" in result.details

    def test_all_invalid_fails(self, spark):
        df = spark.createDataFrame([("bad1",), ("bad2",)], ["status"])
        result = check_allowed_values(df, "status", ["completed"])
        assert not result.passed

    def test_numeric_allowed_values(self, spark):
        df = spark.createDataFrame([(1,), (2,), (5,)], ["rating"])
        result = check_allowed_values(df, "rating", [1, 2, 3, 4, 5])
        assert result.passed


class TestCheckReferentialIntegrity:
    def test_all_references_valid(self, spark):
        orders = spark.createDataFrame(
            [(1, 101), (2, 102)], ["order_id", "customer_id"])
        customers = spark.createDataFrame([(101,), (102,)], ["customer_id"])
        result = check_referential_integrity(
            orders, customers, "customer_id", "customer_id")
        assert result.passed

    def test_detects_orphan_record(self, spark):
        orders = spark.createDataFrame(
            [(1, 101), (2, 999)], ["order_id", "customer_id"])
        customers = spark.createDataFrame([(101,), (102,)], ["customer_id"])
        result = check_referential_integrity(
            orders, customers, "customer_id", "customer_id")
        assert not result.passed
        assert "1 registro" in result.details

    def test_all_orphans_fails(self, spark):
        orders = spark.createDataFrame(
            [(1, 999), (2, 888)], ["order_id", "customer_id"])
        customers = spark.createDataFrame([(101,), (102,)], ["customer_id"])
        result = check_referential_integrity(
            orders, customers, "customer_id", "customer_id")
        assert not result.passed

    def test_empty_child_passes(self, spark):
        schema_o = StructType([
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
        ])
        orders = spark.createDataFrame([], schema_o)
        customers = spark.createDataFrame([(101,)], ["customer_id"])
        result = check_referential_integrity(
            orders, customers, "customer_id", "customer_id")
        assert result.passed
