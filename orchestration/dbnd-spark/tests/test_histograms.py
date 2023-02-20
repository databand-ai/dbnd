# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import logging

import pytest

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from dbnd_spark import get_spark_session
from dbnd_spark.spark_targets.spark_values import SparkDataFrameValueType
from dbnd_test_scenarios.test_common.histogram_tests import (
    BaseHistogramTests,
    get_value_meta_from_value,
)


logger = logging.getLogger(__name__)


@pytest.mark.skip("Spark Historgrams are not supported at the moment")
@pytest.mark.spark
class TestSparkHistograms(BaseHistogramTests):
    def data_to_value(self, data):
        spark_session = get_spark_session()
        data = list(zip(*data))
        column_names = ["test_column_" + str(i) for i in range(len(data[0]))]
        return spark_session.createDataFrame(data, column_names)

    def test_complex_column(self, spark_session, meta_conf, numbers):
        # list is a complex value, and it can't have a histogram,
        # so we want to make sure we handle it correctly and nothing breaks
        complex_column = [
            (i, [str(i), str(i + 1)]) if i else [None] * 2 for i in numbers
        ]
        complex_column = list(zip(*complex_column))
        df = self.data_to_value(complex_column)
        value_meta = get_value_meta_from_value("complex", df, meta_conf)

        assert list(value_meta.histograms.keys()) == ["test_column_0"]
        assert [col_stats.column_name for col_stats in value_meta.columns_stats] == [
            "test_column_0"
        ]
        self.validate_numeric_histogram_and_stats(value_meta, "test_column_0")

    def test_null_int_column(self, spark_session, meta_conf):
        nulls = [(None,) for _ in range(20)]
        schema = StructType([StructField("null_column", IntegerType(), True)])
        null_df = spark_session.createDataFrame(nulls, schema=schema)
        value_meta = SparkDataFrameValueType().get_value_meta(null_df, meta_conf)

        assert value_meta.histograms == {}
        col_stats = value_meta.get_column_stats_by_col_name("null_column")
        assert col_stats.column_type == "integer"

    def test_null_str_column(self, spark_session, meta_conf):
        nulls = [(None,) for _ in range(20)]
        schema = StructType([StructField("null_column", StringType(), True)])
        null_df = spark_session.createDataFrame(nulls, schema=schema)
        value_meta = SparkDataFrameValueType().get_value_meta(null_df, meta_conf)

        assert value_meta.histograms["null_column"] == ([20], [None])
        col_stats = value_meta.get_column_stats_by_col_name("null_column")
        assert col_stats.column_type == "string"
