from __future__ import print_function

import logging

import pytest

from pyspark.sql import SparkSession
from pytest import fixture

from dbnd._core.tracking.histograms import HistogramRequest, HistogramSpec
from dbnd_spark.spark_targets import SparkDataFrameValueType
from targets.value_meta import ValueMetaConf
from targets.values import get_value_type_of_obj
from targets.values.pandas_values import DataFrameValueType


logger = logging.getLogger(__name__)


class TestHistograms:
    @fixture
    def spark_session(self):
        spark = SparkSession.builder.appName(
            "Spark dataframe histogram test"
        ).getOrCreate()
        return spark

    @fixture
    def meta_conf(self):
        return ValueMetaConf.enabled()

    @pytest.mark.skip
    def test_boolean_histogram(self, spark_session, meta_conf):
        booleans = [True] * 10 + [None] * 10 + [False] * 20 + [True] * 20
        booleans = [(i,) for i in booleans]
        boolean_df = spark_session.createDataFrame(booleans, ["boolean_column"])

        stats, histograms = SparkDataFrameValueType().get_histograms(
            boolean_df, meta_conf
        )

        histogram = histograms["boolean_column"]
        assert histogram[0] == [30, 20, 10]
        assert histogram[1] == [True, False, None]

        stats = stats["boolean_column"]
        assert set(stats.keys()) == set(
            ["type", "distinct", "null-count", "non-null", "count",]
        )
        assert stats["count"] == 60
        assert stats["non-null"] == 50
        assert stats["null-count"] == 10
        assert stats["distinct"] == 2
        assert stats["type"] == "boolean"

    @pytest.mark.skip
    def test_numerical_histogram(self, spark_session, meta_conf):
        numbers = [1, 3, 3, 1, 5, 1, 5, 5]
        numbers = [(i,) for i in numbers]
        df = spark_session.createDataFrame(numbers, ["numerical_column"])

        stats, histograms = SparkDataFrameValueType().get_histograms(df, meta_conf)
        # pandas_df = df.toPandas()
        # pandas_stats, pandas_histograms = DataFrameValueType().get_histograms(pandas_df, meta_conf)

        stats = stats["numerical_column"]
        assert list(stats.keys()) == [
            "count",
            "mean",
            "stddev",
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
        ]
        assert stats["count"] == 8
        assert stats["non-null"] == 8
        assert stats["distinct"] == 3
        assert stats["min"] == 1
        assert stats["max"] == 5
        assert stats["type"] == "long"

    @pytest.mark.skip
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

        stats, histograms = SparkDataFrameValueType().get_histograms(df, meta_conf)

        histogram = histograms["string_column"]
        assert histogram[0] == [30, 20, 15, 5]
        assert histogram[1] == ["Ola Mundo!", "Shalom Olam!", "Hello World!", None]

        stats = stats["string_column"]
        assert set(stats.keys()) == set(
            ["type", "distinct", "null-count", "non-null", "count"]
        )
        assert stats["count"] == 70
        assert stats["non-null"] == 65
        assert stats["null-count"] == 5
        assert stats["distinct"] == 3
        assert stats["type"] == "string"

    @pytest.mark.skip
    def test_histogram_others(self, spark_session, meta_conf):
        strings = []
        for i in range(1, 101):
            str = "str-{}".format(i)
            new_strings = [str] * i
            strings.extend(new_strings)

        strings = [(i,) for i in strings]
        df = spark_session.createDataFrame(strings, ["string_column"])

        stats, histograms = SparkDataFrameValueType().get_histograms(df, meta_conf)

        histogram = histograms["string_column"]
        assert len(histogram[0]) == 50 and len(histogram[1]) == 50
        assert histogram[0][0] == 100 and histogram[1][0] == "str-100"
        assert histogram[0][10] == 90 and histogram[1][10] == "str-90"
        assert histogram[0][-2] == 52 and histogram[1][-2] == "str-52"
        assert histogram[0][-1] == sum(range(1, 52)) and histogram[1][-1] == "_others"

        stats = stats["string_column"]
        assert stats["count"] == 5050 == sum(histogram[0])
        assert stats["non-null"] == 5050
        assert stats["null-count"] == 0
        assert stats["distinct"] == 100
        assert stats["type"] == "string"
