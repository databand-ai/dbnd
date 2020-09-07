from __future__ import absolute_import

import logging
import os
import time
import typing

from collections import defaultdict

import pyspark.sql as spark

from pyspark import Row
from pyspark.sql.functions import (
    approx_count_distinct,
    col,
    count,
    countDistinct,
    desc,
    floor,
    isnull,
    lit,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegralType,
    MapType,
    NumericType,
    StringType,
    StructType,
)

from dbnd import config
from dbnd._core.settings.histogram import HistogramConfig
from dbnd._core.utils import seven


if typing.TYPE_CHECKING:
    from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger(__name__)


class SparkHistograms(object):
    def __init__(self, histogram_spec):
        self.histogram_spec = histogram_spec
        self.metrics = dict()
        self._temp_parquet_path = None
        self.config = HistogramConfig()  # type: HistogramConfig

    def get_histograms(self, df):
        # type: (spark.DataFrame) -> Tuple[Dict[str, Dict], Dict[str, Tuple]]
        df_cached = False
        try:
            if self.histogram_spec.none:
                return {}, {}

            df = self._filter_complex_columns(df).select(
                list(self.histogram_spec.columns)
            )

            if self.config.spark_parquet_cache_dir:
                df = self._cache_df_with_parquet_store(
                    df, spark_parquet_cache_dir=self.config.spark_parquet_cache_dir
                )
            if self.config.spark_cache_dataframe:
                df_cached = True
                df.cache()

            summary = {}
            if self.histogram_spec.only_stats or self.histogram_spec.with_stats:
                with self._measure_time("summary_calc_time"):
                    summary = self._calculate_summary(df)

            if self.histogram_spec.only_stats:
                return summary, {}

            with self._measure_time("histograms_calc_time"):
                histograms = self._calculate_histograms(df)

            return summary, histograms
        except Exception:
            logger.exception("Error occured during histograms calculation")
            return {}, {}
        finally:
            if self._temp_parquet_path:
                self._remove_parquet(df.sql_ctx.sparkSession)
            if df_cached:
                df.unpersist()

    def _cache_df_with_parquet_store(self, df, spark_parquet_cache_dir):
        """ save dataframe as column-based parquet file to allow fast column queries which histograms depend on """
        from dbnd_spark.spark_targets import SparkDataFrameValueType

        signature = SparkDataFrameValueType().to_signature(df)
        file_name = "dbnd_spark_dataframe_{}.parquet".format(signature)
        path = os.path.join(spark_parquet_cache_dir, file_name)

        self._temp_parquet_path = path
        logger.info("Caching spark dataframe into '%s'.", path)
        df.write.parquet(path)

        logger.info("Reading spark dataframe from '%s'.", path)
        df = df.sql_ctx.sparkSession.read.parquet(path)
        return df

    def _remove_parquet(self, spark_session):
        # we are not able to delete generically files so we overwrite them with almost no data
        empty_df = spark_session.createDataFrame([("",)], [""])
        empty_df.write.parquet(self._temp_parquet_path, mode="overwrite")

    def _filter_complex_columns(self, df):
        simple_columns = []
        for column_def in df.schema.fields:
            if isinstance(column_def.dataType, (ArrayType, MapType, StructType)):
                logger.warning(
                    "Column %s was ignored in histogram calculation as it contains complex type (%s)",
                    column_def.name,
                    column_def.dataType,
                )
                continue
            simple_columns.append(column_def.name)
        return df.select(simple_columns)

    def _is_count_in_summary(self, dataframe, column_name):
        """ dataframe.summary() returns count only for numeric and string types, otherwise we need to calculate it our own """
        column_field = [f for f in dataframe.schema.fields if f.name == column_name][0]
        return isinstance(column_field.dataType, (NumericType, StringType))

    def _calculate_summary(self, df):
        counts = self._query_counts(df)
        summary = df.summary().collect()
        result = {column_name: {} for column_name in df.columns}

        for column_def in df.schema.fields:
            column_name = column_def.name
            if isinstance(column_def.dataType, NumericType):
                # summary (min, max, mean, std, etc.) make sense only for numeric types
                for row in summary:
                    if row[column_name] is None:
                        continue
                    # zero cell contains summary metrics name
                    result[column_name][row[0]] = float(row[column_name])
                if "stddev" in result[column_name]:
                    result[column_name]["std"] = result[column_name]["stddev"]
            elif isinstance(column_def.dataType, StringType):
                count_row = [row for row in summary if row[0] == "count"][0]
                result[column_name]["count"] = int(count_row[column_name])
            else:
                result[column_name]["count"] = counts["%s_count" % column_name]

            result[column_name]["type"] = column_def.dataType.jsonValue()
            result[column_name]["distinct"] = counts["%s_distinct" % column_name]
            result[column_name]["null-count"] = counts["%s_count_null" % column_name]
            # count in summary calculates only non-null counts, so we have to summarize non-null and null
            result[column_name]["non-null"] = result[column_name]["count"]
            result[column_name]["count"] += result[column_name]["null-count"]
        return result

    def _query_counts(self, df):
        """
        non-null and distinct counts should be calculated in separate query, because summary doesn't includes it
        """
        if self.histogram_spec.approx_distinct_count:
            count_distinct_function = approx_count_distinct
        else:
            count_distinct_function = countDistinct

        expressions = (
            [
                count_distinct_function(col(c)).alias("%s_distinct" % c)
                for c in df.columns
            ]
            + [count(when(isnull(c), c)).alias("%s_count_null" % c) for c in df.columns]
            + [
                count(col(c)).alias("%s_count" % c)
                for c in df.columns
                if not self._is_count_in_summary(df, c)
            ]
        )
        counts = df.agg(*expressions).collect()[0]
        return counts

    def _calculate_histograms(self, df):
        # type: (spark.DataFrame) -> Dict
        histograms = {column_name: None for column_name in df.columns}

        with self._measure_time("boolean_histograms_calc_time"):
            boolean_histograms = self._calculate_categorical_histograms_by_type(
                df, BooleanType
            )
        histograms.update(boolean_histograms)

        with self._measure_time("string_histograms_calc_time"):
            str_histograms = self._calculate_categorical_histograms_by_type(
                df, StringType
            )
        histograms.update(str_histograms)

        with self._measure_time("numerical_histograms_calc_time"):
            numeric_histograms = self._calculate_numerical_histograms(df)
        histograms.update(numeric_histograms)
        return histograms

    def _get_columns_by_type(self, dataframe, column_type):
        return [
            dataframe.select(f.name)
            for f in dataframe.schema.fields
            if isinstance(f.dataType, column_type)
        ]

    def _calculate_numerical_histograms(self, df):
        column_df_list = self._get_columns_by_type(df, NumericType)
        if not column_df_list:
            return dict()

        value_counts = []
        summary = dict()
        for column_df in column_df_list:
            column_df_cached = False

            column_name = column_df.schema.names[0]
            if self.config.spark_cache_dataframe_column:
                column_df_cached = True
                column_df.cache()
            try:
                column_summary = column_df.summary("min", "max").collect()
                summary[column_name] = dict()
                for summary_row in column_summary:
                    if summary_row[column_name] is None:
                        break
                    summary[column_name][summary_row.summary] = float(
                        summary_row[column_name]
                    )

                if (
                    "min" not in summary[column_name]
                    or "max" not in summary[column_name]
                ):
                    logger.warning(
                        "Failed to calculate min/max for column '%s', skipping histogram calculation",
                        column_name,
                    )
                    continue

                column_value_counts = self._get_column_numerical_buckets_df(
                    column_df,
                    summary[column_name]["min"],
                    summary[column_name]["max"],
                    20,
                )
                column_value_counts = column_value_counts.withColumn(
                    "column_name", lit(column_name)
                )
                column_value_counts = column_value_counts.collect()
            finally:
                if column_df_cached:
                    column_df.unpersist()
            value_counts.extend(column_value_counts)

        histograms = self._convert_numerical_histogram_collect_to_dict(
            value_counts, summary
        )
        return histograms

    def _convert_numerical_histogram_collect_to_dict(self, value_counts, summary):
        # type: (List[Row], Dict) -> Dict
        """
        histogram df is list of rows with each row representing a bucket.
        each bucket has: bucket index, number of values, column name.
        we convert it to a dict with column names as keys and histograms as values.
        histogram is represented as a tuple of 2 lists: number of values in bucket and bucket boundaries.
        """
        histogram_dict = dict()
        for row in value_counts:
            bucket, count, column_name = row
            if column_name not in histogram_dict:
                bucket_count = 20
                counts = [0] * bucket_count
                min_value = summary[column_name]["min"]
                max_value = summary[column_name]["max"]
                bucket_size = (max_value - min_value) / bucket_count
                values = [min_value + i * bucket_size for i in range(bucket_count + 1)]
                histogram_dict[column_name] = (counts, values)
            if bucket is None:
                continue
            if bucket == bucket_count:
                # handle edge of last bucket (values equal to max_value will be in bucket n+1 instead of n)
                bucket = bucket - 1
            histogram_dict[column_name][0][bucket] = count
        return histogram_dict

    def _get_column_numerical_buckets_df(
        self, column_df, min_value, max_value, bucket_count
    ):
        min_value, max_value = float(min_value), float(max_value)
        first_bucket = min_value
        bucket_size = (max_value - min_value) / bucket_count
        df_with_bucket = column_df.withColumn(
            "bucket", floor((column_df[0] - first_bucket) / bucket_size)
        )
        counts_df = df_with_bucket.groupby("bucket").count()
        return counts_df

    def _calculate_categorical_histograms_by_type(self, dataframe, column_type):
        column_df_list = self._get_columns_by_type(dataframe, column_type)
        if not column_df_list:
            return dict()

        value_counts = self._spark_categorical_histograms(column_df_list)
        histograms = self._convert_categorical_histogram_collect_to_dict(value_counts)
        if column_type == StringType:
            self._add_others(histograms, column_df_list)
        return histograms

    def _spark_categorical_histograms(self, column_df_list):
        """ all columns in column_df_list should have the same type (e.g. all should be booleans or all strings) """
        max_buckets = 50
        value_counts = []
        for column_df in column_df_list:
            column_name = column_df.schema.names[0]
            column_value_counts = (
                column_df.groupby(column_name)
                .count()
                .orderBy(desc("count"))
                .withColumn("column_name", lit(column_name))
                .limit(max_buckets - 1)
            )
            column_value_counts = column_value_counts.collect()
            value_counts.extend(column_value_counts)
        return value_counts

    def _add_others(self, histograms, column_df_list):
        """ sum all least significant values (who left out of histogram) to one bucket """
        if not column_df_list:
            return

        total_count = column_df_list[0].count()
        for column_name, histogram in histograms.items():
            histogram_sum_count = sum(histogram[0])
            others_count = total_count - histogram_sum_count
            if others_count > 0:
                histogram[0].append(others_count)
                histogram[1].append("_others")

    def _convert_categorical_histogram_collect_to_dict(self, value_counts):
        # type: (List[Row], Dict) -> Dict
        histogram_dict = defaultdict(lambda: ([], []))
        for row in value_counts:
            value, count, column_name = row
            histogram_dict[column_name][0].append(count)
            histogram_dict[column_name][1].append(value)
        return histogram_dict

    @seven.contextlib.contextmanager
    def _measure_time(self, metric_key):
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            self.metrics[metric_key] = end_time - start_time
