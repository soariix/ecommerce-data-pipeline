from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from src.transform import add_order_total, cast_dates, normalize_status, remove_duplicates


class TestCastDates:
    def test_converts_string_to_date(self, spark):
        df = spark.createDataFrame([("2024-01-15",)], ["order_date"])
        result = cast_dates(df, ["order_date"])
        assert isinstance(result.schema["order_date"].dataType, DateType)

    def test_correct_date_value(self, spark):
        import datetime
        df = spark.createDataFrame([("2024-03-10",)], ["order_date"])
        result = cast_dates(df, ["order_date"])
        row = result.collect()[0]
        assert row["order_date"] == datetime.date(2024, 3, 10)

    def test_invalid_date_becomes_null(self, spark):
        df = spark.createDataFrame([("not-a-date",)], ["order_date"])
        result = cast_dates(df, ["order_date"])
        assert result.collect()[0]["order_date"] is None

    def test_casts_multiple_date_columns(self, spark):
        df = spark.createDataFrame(
            [("2023-06-01", "2024-01-15")],
            ["signup_date", "last_purchase_date"],
        )
        result = cast_dates(df, ["signup_date", "last_purchase_date"])
        assert isinstance(result.schema["signup_date"].dataType, DateType)
        assert isinstance(
            result.schema["last_purchase_date"].dataType, DateType)

    def test_custom_date_format(self, spark):
        import datetime
        df = spark.createDataFrame([("15/01/2024",)], ["order_date"])
        result = cast_dates(df, ["order_date"], fmt="dd/MM/yyyy")
        assert result.collect()[0]["order_date"] == datetime.date(2024, 1, 15)


class TestNormalizeStatus:
    def test_lowercases_uppercase(self, spark):
        df = spark.createDataFrame([("COMPLETED",)], ["status"])
        result = normalize_status(df, "status")
        assert result.collect()[0]["status"] == "completed"

    def test_trims_leading_trailing_spaces(self, spark):
        df = spark.createDataFrame([("  cancelled  ",)], ["status"])
        result = normalize_status(df, "status")
        assert result.collect()[0]["status"] == "cancelled"

    def test_mixed_case_with_spaces(self, spark):
        df = spark.createDataFrame([("  Processing  ",)], ["status"])
        result = normalize_status(df, "status")
        assert result.collect()[0]["status"] == "processing"

    def test_already_lowercase_unchanged(self, spark):
        df = spark.createDataFrame([("completed",)], ["status"])
        result = normalize_status(df, "status")
        assert result.collect()[0]["status"] == "completed"

    def test_null_status_remains_null(self, spark):
        schema = StructType([StructField("status", StringType(), True)])
        df = spark.createDataFrame([(None,)], schema)
        result = normalize_status(df, "status")
        assert result.collect()[0]["status"] is None


class TestRemoveDuplicates:
    def test_removes_exact_duplicates(self, spark):
        df = spark.createDataFrame(
            [(1, "a"), (1, "a"), (2, "b")], ["id", "val"])
        result = remove_duplicates(df, ["id"])
        assert result.count() == 2

    def test_keeps_all_unique_rows(self, spark):
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        result = remove_duplicates(df, ["id"])
        assert result.count() == 3

    def test_composite_key_deduplication(self, spark):
        df = spark.createDataFrame(
            [(1, 10), (1, 10), (1, 20), (2, 10)],
            ["order_id", "product_id"],
        )
        result = remove_duplicates(df, ["order_id", "product_id"])
        assert result.count() == 3

    def test_empty_dataframe_unchanged(self, spark):
        schema = StructType([StructField("id", IntegerType(), True)])
        df = spark.createDataFrame([], schema)
        result = remove_duplicates(df, ["id"])
        assert result.count() == 0


class TestAddOrderTotal:
    _schema = StructType([
        StructField("quantity", IntegerType(), True),
        StructField("price",    DoubleType(),  True),
    ])

    def test_calculates_order_total_correctly(self, spark):
        df = spark.createDataFrame([(2, 149.99)], self._schema)
        result = add_order_total(df)
        assert abs(result.collect()[0]["order_total"] - 299.98) < 0.01

    def test_single_item_order(self, spark):
        df = spark.createDataFrame([(1, 89.99)], self._schema)
        result = add_order_total(df)
        assert abs(result.collect()[0]["order_total"] - 89.99) < 0.01

    def test_rounds_to_two_decimal_places(self, spark):
        df = spark.createDataFrame([(3, 10.005)], self._schema)
        result = add_order_total(df)
        total = result.collect()[0]["order_total"]
        # Verifica que tem no máximo 2 casas decimais
        assert round(total, 2) == total

    def test_adds_column_to_dataframe(self, spark):
        df = spark.createDataFrame([(1, 50.0)], self._schema)
        result = add_order_total(df)
        assert "order_total" in result.columns

    def test_null_quantity_produces_null_total(self, spark):
        df = spark.createDataFrame([(None, 50.0)], self._schema)
        result = add_order_total(df)
        assert result.collect()[0]["order_total"] is None
