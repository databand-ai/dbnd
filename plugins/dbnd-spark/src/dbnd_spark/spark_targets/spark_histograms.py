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
    MapType,
    NumericType,
    StringType,
    StructType,
)

from dbnd._core.settings.histogram import HistogramConfig
from dbnd._core.utils import seven


if typing.TYPE_CHECKING:
    from typing import Tuple, Dict, List
    from targets.value_meta import ValueMetaConf
    from pyspark.sql.dataframe import DataFrame

logger = logging.getLogger(__name__)


class SparkHistograms(object):
    """
    calculates histograms and stats on spark dataframe.
    they're calculated together since we do it per column and we use cache.
    """

    def __init__(self, df, meta_conf):
        self.df = df  # type: DataFrame
        self.meta_conf = meta_conf  # type: ValueMetaConf
        self.config = HistogramConfig()  # type: HistogramConfig
        self.system_metrics = dict()
        self.stats = dict()
        self.histograms = dict()
        self._temp_parquet_path = None
        self._histogram_column_names = None  # columns to calc histograms on
        self._stats_column_names = None  # columns to calc stats on

    def get_histograms_and_stats(self):
        # type: () -> Tuple[Dict[str, Dict], Dict[str, Tuple]]
        if self.stats or self.histograms:
            return self.stats, self.histograms

        df_cached = False
        df = None
        try:
            df = self._filter_columns(self.df)
            if self.config.spark_parquet_cache_dir:
                df = self._cache_df_with_parquet_store(
                    df, spark_parquet_cache_dir=self.config.spark_parquet_cache_dir
                )
            if self.config.spark_cache_dataframe:
                df_cached = True
                df.cache()

            with self._measure_time("histograms_and_stats_calc_time"):
                self.histograms = self._calc_histograms_and_stats(df)

            return self.stats, self.histograms
        except Exception:
            logger.exception("Error occured during histograms calculation")
            return {}, {}
        finally:
            if self._temp_parquet_path and df:
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

    def _filter_columns(self, df):
        self._histogram_column_names = self._get_column_names_from_request(
            df, self.meta_conf.log_histograms
        )
        self._stats_column_names = self._get_column_names_from_request(
            df, self.meta_conf.log_stats
        )
        column_names = list(
            set(self._histogram_column_names + self._stats_column_names)
        )
        df = df.select(column_names)
        df = self._filter_complex_columns(df)
        return df

    def _get_column_names_from_request(self, df, data_request):
        column_names = list(data_request.include_columns)
        for column_def in df.schema:
            if data_request.include_all_string and isinstance(
                column_def.dataType, StringType
            ):
                column_names.append(column_def.name)
            elif data_request.include_all_boolean and isinstance(
                column_def.dataType, BooleanType
            ):
                column_names.append(column_def.name)
            elif data_request.include_all_numeric and isinstance(
                column_def.dataType, NumericType
            ):
                column_names.append(column_def.name)

        column_names = [
            column
            for column in column_names
            if column not in data_request.exclude_columns
        ]
        return column_names

    def _filter_complex_columns(self, df):
        simple_columns = []
        for column_def in df.schema:
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

    def _calc_histograms_and_stats(self, df):
        # type: (spark.DataFrame) -> Dict
        histograms = dict()
        df_count = df.count()
        for column_schema in df.schema:
            self.stats[column_schema.name] = dict(
                count=df_count, type=column_schema.dataType.jsonValue()
            )

        with self._measure_time("boolean_histograms_and_stats_calc_time"):
            boolean_histograms = self._calc_categorical_hist_and_stats_by_type(
                df, BooleanType
            )
        histograms.update(boolean_histograms)

        with self._measure_time("string_histograms_and_stats_calc_time"):
            str_histograms = self._calc_categorical_hist_and_stats_by_type(
                df, StringType
            )
        histograms.update(str_histograms)

        with self._measure_time("numeric_histograms_and_stats_calc_time"):
            numeric_histograms = self._calc_numeric_hist_and_stats(df)
        histograms.update(numeric_histograms)
        return histograms

    def _get_columns_by_type(self, dataframe, column_type):
        return [
            dataframe.select(f.name)
            for f in dataframe.schema
            if isinstance(f.dataType, column_type)
        ]

    def _calc_numeric_hist_and_stats(self, df):
        column_df_list = self._get_columns_by_type(df, NumericType)
        if not column_df_list:
            return dict()

        histograms = dict()
        for column_df in column_df_list:
            column_df_cached = False
            if self.config.spark_cache_dataframe_column:
                column_df_cached = True
                column_df.cache()
            try:
                column_name = column_df.schema.names[0]
                self._calc_numeric_column_stats(column_df, column_name)

                if column_name not in self._histogram_column_names:
                    continue
                if (
                    "min" not in self.stats[column_name]
                    or "max" not in self.stats[column_name]
                ):
                    logger.warning(
                        "Failed to calculate min/max for column '%s', skipping histogram calculation",
                        column_name,
                    )
                    continue

                column_histograms = self._calc_numeric_column_histogram(
                    column_df, column_name
                )
                histograms[column_name] = column_histograms
            finally:
                if column_df_cached:
                    column_df.unpersist()
        return histograms

    def _calc_numeric_column_histogram(self, column_df, column_name):
        min_value = self.stats[column_name]["min"]
        max_value = self.stats[column_name]["max"]
        value_counts = self._get_column_numeric_buckets_df(
            column_df, min_value, max_value, 20,
        )
        value_counts = value_counts.collect()
        column_histograms = self._convert_numeric_histogram_collect_to_tuple(
            value_counts, min_value, max_value
        )
        return column_histograms

    def _calc_numeric_column_stats(self, column_df, column_name):
        if column_name in self._stats_column_names:
            column_summary = column_df.summary().collect()
        else:
            # min & max are required for histogram calculation
            column_summary = column_df.summary("min", "max").collect()

        stats = self.stats[column_name]
        for summary_row in column_summary:
            if summary_row[column_name] is None:
                continue
            if summary_row.summary == "count":
                stats["non-null"] = float(summary_row[column_name])
            else:
                stats[summary_row.summary] = float(summary_row[column_name])
        if "stddev" in stats:
            stats["std"] = stats.pop("stddev")

        if column_name not in self._stats_column_names:
            return

        # count in summary doesn't include nulls, while count() function does
        stats["null-count"] = stats["count"] - stats["non-null"]
        stats["distinct"] = column_df.distinct().count()

    def _convert_numeric_histogram_collect_to_tuple(
        self, value_counts, min_value, max_value
    ):
        # type: (List[Row], float, float) -> Tuple
        """
        value_counts is list of rows with each row representing a bucket.
        each bucket has: bucket index and number of values.
        we convert it to histogram represented as a tuple of 2 lists:
        number of values in bucket and bucket boundaries.
        """
        bucket_count = 20
        counts = [0] * bucket_count
        bucket_size = (max_value - min_value) / bucket_count
        values = [min_value + i * bucket_size for i in range(bucket_count + 1)]

        for row in value_counts:
            bucket, count = row
            if bucket is None:
                continue
            if bucket == bucket_count:
                # handle edge of last bucket (values equal to max_value will be in bucket n+1 instead of n)
                bucket = bucket - 1
            counts[bucket] += count

        return counts, values

    def _get_column_numeric_buckets_df(
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

    def _calc_categorical_hist_and_stats_by_type(self, dataframe, column_type):
        column_df_list = self._get_columns_by_type(dataframe, column_type)
        if not column_df_list:
            return dict()

        value_counts = self._calc_spark_categorical_hist_and_stats(column_df_list)
        histograms = self._convert_categorical_histogram_collect_to_dict(value_counts)
        if column_type == StringType:
            self._add_others(histograms)
        return histograms

    def _calc_spark_categorical_hist_and_stats(self, column_df_list):
        """
        all columns in column_df_list should have the same type (e.g. all should be booleans or all strings).
        it might not be relevant anymore since we do collect() per column.
        keeping it that way for now so we could change it back to collect() once for all columns.
        """
        max_buckets = 50
        value_counts = []
        for column_df in column_df_list:
            if self.config.spark_cache_dataframe_column:
                column_df_cached = True
                column_df.cache()
            else:
                column_df_cached = False
            try:
                column_name = column_df.schema.names[0]
                self._calc_categorical_column_stats(column_df, column_name)
                if column_name not in self._histogram_column_names:
                    continue
                column_value_counts = (
                    column_df.groupby(column_name)
                    .count()
                    .orderBy(desc("count"))
                    .withColumn("column_name", lit(column_name))
                    .limit(max_buckets - 1)
                )
                column_value_counts = column_value_counts.collect()
                value_counts.extend(column_value_counts)
            finally:
                if column_df_cached:
                    column_df.unpersist()
        return value_counts

    def _calc_categorical_column_stats(self, column_df, column_name):
        if column_name not in self._histogram_column_names:
            return
        if isinstance(column_df.schema[0].dataType, BooleanType):
            return

        # count in summary doesn't include nulls, while count() function does
        column_summary = column_df.summary("count").collect()
        stats = self.stats[column_name]
        stats["non-null"] = int(column_summary[0][column_name])
        stats["null-count"] = stats["count"] - stats["non-null"]
        stats["distinct"] = column_df.distinct().count()

    def _add_others(self, histograms):
        """ sum all least significant values (who left out of histogram) to one bucket """
        for column_name, histogram in histograms.items():
            histogram_sum_count = sum(histogram[0])
            others_count = self.stats[column_name]["count"] - histogram_sum_count
            if others_count > 0:
                histogram[0].append(others_count)
                histogram[1].append("_others")

    def _convert_categorical_histogram_collect_to_dict(self, value_counts):
        # type: (List[Row]) -> Dict
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
            self.system_metrics[metric_key] = end_time - start_time
