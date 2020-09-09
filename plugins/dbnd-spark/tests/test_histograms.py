from __future__ import print_function

import logging

from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pytest import fixture

from dbnd._core.tracking.histograms import HistogramRequest
from dbnd_spark.spark_targets import SparkDataFrameValueType
from targets.value_meta import ValueMetaConf


logger = logging.getLogger(__name__)


class TestHistograms:
    @fixture
    def meta_conf(self):
        conf = ValueMetaConf.enabled()
        conf.log_histograms = HistogramRequest.ALL()
        return conf

    @fixture
    def numbers(self):
        return [1, 1, 3, 1, 5, None, 1, 5, 5, None]

    def validate_numerical_histogram_and_stats(self, value_meta, column_name):
        """ assuming numbers fixture is used """
        assert column_name in value_meta.histograms
        histogram = value_meta.histograms[column_name]
        assert len(histogram) == 2
        assert len(histogram[0]) == 20
        assert len(histogram[1]) == 21
        assert sum(histogram[0]) == 8

        stats = value_meta.descriptive_stats[column_name]
        assert set(stats.keys()) == {
            "count",
            "mean",
            "min",
            "25%",
            "50%",
            "75%",
            "max",
            "std",
            "type",
            "distinct",
            "null-count",
            "non-null",
        }
        assert stats["count"] == 10
        assert stats["non-null"] == 8
        assert stats["distinct"] == 4
        assert stats["min"] == 1
        assert stats["max"] == 5

    def test_boolean_histogram(self, spark_session, meta_conf):
        booleans = [True] * 10 + [None] * 10 + [False] * 20 + [True] * 20
        booleans = [(i,) for i in booleans]
        boolean_df = spark_session.createDataFrame(booleans, ["boolean_column"])

        value_meta = SparkDataFrameValueType().get_value_meta(boolean_df, meta_conf)

        histogram = value_meta.histograms["boolean_column"]
        assert histogram[0] == [30, 20, 10]
        assert histogram[1] == [True, False, None]

        stats = value_meta.descriptive_stats["boolean_column"]
        assert stats["count"] == 60
        assert stats["type"] == "boolean"

    def test_int_column(self, spark_session, meta_conf, numbers):
        numbers = [(i,) for i in numbers]
        df = spark_session.createDataFrame(numbers, ["numerical_column"])
        value_meta = SparkDataFrameValueType().get_value_meta(df, meta_conf)
        # pandas_df = df.toPandas()
        # pandas_stats, pandas_histograms = DataFrameValueType().get_histograms(pandas_df, meta_conf)
        self.validate_numerical_histogram_and_stats(value_meta, "numerical_column")
        stats = value_meta.descriptive_stats["numerical_column"]
        assert stats["type"] == "long"

    def test_float_column(self, spark_session, meta_conf, numbers):
        numbers = [(float(i),) if i else (None,) for i in numbers]
        df = spark_session.createDataFrame(numbers, ["numerical_column"])
        value_meta = SparkDataFrameValueType().get_value_meta(df, meta_conf)
        # pandas_df = df.toPandas()
        # pandas_stats, pandas_histograms = DataFrameValueType().get_histograms(pandas_df, meta_conf)
        self.validate_numerical_histogram_and_stats(value_meta, "numerical_column")
        stats = value_meta.descriptive_stats["numerical_column"]
        assert stats["type"] == "double"

    def test_strings_histogram(self, spark_session, meta_conf):
        strings = (
            ["Hello World!"] * 15
            + [None] * 5
            + ["Ola Mundo!"] * 15
            + ["Shalom Olam!"] * 20
            + ["Ola Mundo!"] * 15
        )
        strings = [(i,) for i in strings]
        df = spark_session.createDataFrame(strings, ["string_column"])

        value_meta = SparkDataFrameValueType().get_value_meta(df, meta_conf)

        histogram = value_meta.histograms["string_column"]
        assert histogram[0] == [30, 20, 15, 5]
        assert histogram[1] == ["Ola Mundo!", "Shalom Olam!", "Hello World!", None]

        stats = value_meta.descriptive_stats["string_column"]
        assert set(stats.keys()) == {
            "type",
            "distinct",
            "null-count",
            "non-null",
            "count",
        }
        assert stats["count"] == 70
        assert stats["non-null"] == 65
        assert stats["null-count"] == 5
        assert stats["distinct"] == 4
        assert stats["type"] == "string"

    def test_histogram_others(self, spark_session, meta_conf):
        strings = []
        for i in range(1, 101):
            str = "str-{}".format(i)
            new_strings = [str] * i
            strings.extend(new_strings)

        strings = [(i,) for i in strings]
        df = spark_session.createDataFrame(strings, ["string_column"])

        value_meta = SparkDataFrameValueType().get_value_meta(df, meta_conf)

        histogram = value_meta.histograms["string_column"]
        assert len(histogram[0]) == 50 and len(histogram[1]) == 50
        assert histogram[0][0] == 100 and histogram[1][0] == "str-100"
        assert histogram[0][10] == 90 and histogram[1][10] == "str-90"
        assert histogram[0][-2] == 52 and histogram[1][-2] == "str-52"
        assert histogram[0][-1] == sum(range(1, 52)) and histogram[1][-1] == "_others"

        stats = value_meta.descriptive_stats["string_column"]
        assert stats["count"] == 5050 == sum(histogram[0])
        assert stats["non-null"] == 5050
        assert stats["null-count"] == 0
        assert stats["distinct"] == 100
        assert stats["type"] == "string"

    def test_complex_column(self, spark_session, meta_conf, numbers):
        complex = [(i, [str(i), str(i + 1)]) if i else [None] * 2 for i in numbers]
        df = spark_session.createDataFrame(
            complex, ["numerical_column", "complex_column"]
        )
        value_meta = SparkDataFrameValueType().get_value_meta(df, meta_conf)

        assert list(value_meta.histograms.keys()) == ["numerical_column"]
        assert list(value_meta.descriptive_stats.keys()) == ["numerical_column"]
        self.validate_numerical_histogram_and_stats(value_meta, "numerical_column")

    def test_null_int_column(self, spark_session, meta_conf):
        column_name = "null_column"
        nulls = [(None,) for _ in range(20)]
        schema = StructType([StructField(column_name, IntegerType(), True)])
        null_df = spark_session.createDataFrame(nulls, schema=schema)
        meta_conf.log_histograms = HistogramRequest.ALL()
        value_meta = SparkDataFrameValueType().get_value_meta(null_df, meta_conf)

        assert value_meta.histograms == {}
        stats = value_meta.descriptive_stats[column_name]
        assert stats["type"] == "integer"

    def test_null_str_column(self, spark_session, meta_conf):
        column_name = "null_column"
        nulls = [(None,) for _ in range(20)]
        schema = StructType([StructField(column_name, StringType(), True)])
        null_df = spark_session.createDataFrame(nulls, schema=schema)
        value_meta = SparkDataFrameValueType().get_value_meta(null_df, meta_conf)
        assert value_meta.histograms[column_name] == ([20], [None])
        stats = value_meta.descriptive_stats[column_name]
        assert stats["type"] == "string"

    def test_multiple_columns(self, spark_session, meta_conf, numbers):
        values = [(i, float(i), str(i), str(i)) if i else [None] * 4 for i in numbers]
        df = spark_session.createDataFrame(values, ["ints", "floats", "str1", "str2"])
        value_meta = SparkDataFrameValueType().get_value_meta(df, meta_conf)

        self.validate_numerical_histogram_and_stats(value_meta, "ints")
        self.validate_numerical_histogram_and_stats(value_meta, "floats")
        str_histogram_1 = value_meta.histograms["str1"]
        str_histogram_2 = value_meta.histograms["str2"]
        assert str_histogram_1[0] == [4, 3, 2, 1]
        assert str_histogram_1[1] == ["1", "5", None, "3"]
        assert str_histogram_1 == str_histogram_2
