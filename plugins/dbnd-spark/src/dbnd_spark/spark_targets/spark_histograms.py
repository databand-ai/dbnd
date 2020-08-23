from __future__ import absolute_import

import logging
import time
import typing

from collections import defaultdict

import pyspark.sql as spark

from pyspark.sql.functions import (
    approx_count_distinct,
    col,
    count,
    countDistinct,
    desc,
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

from dbnd._core.utils import seven


if typing.TYPE_CHECKING:
    from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger(__name__)


class SparkHistograms(object):
    def __init__(self, histogram_spec):
        self.histogram_spec = histogram_spec
        self.metrics = dict()

    def get_histograms(self, df):
        # type: (spark.DataFrame) -> Tuple[Optional[Dict[Dict[str, Any]]], Optional[Dict[str, Tuple]]]
        try:
            if self.histogram_spec.none:
                return None, None

            df = self._filter_complex_columns(df).select(
                list(self.histogram_spec.columns)
            )
            with self._measure_time("summary_calc_time"):
                summary = self._calculate_summary(df)

            if self.histogram_spec.only_stats:
                return summary, {}

            with self._measure_time("histograms_calc_time"):
                histograms = self._calculate_histograms(df, summary)
            return summary, histograms
        except Exception:
            logger.exception("Error occured during histograms calculation")
            return None, None

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

    def _count_in_summary(self, dataframe, column_name):
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
                    # zero cell contains summary metrics name
                    result[column_name][row[0]] = float(row[column_name])
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
                if not self._count_in_summary(df, c)
            ]
        )
        counts = df.agg(*expressions).collect()[0]
        return counts

    def _calculate_histograms(self, df, summary):
        # type: (spark.DataFrame, Dict) -> Dict
        histograms = {column_name: None for column_name in df.columns}

        with self._measure_time("boolean_histograms_calc_time"):
            boolean_histograms = self._calculate_categorical_histograms_by_type(
                df, BooleanType, summary
            )
        histograms.update(boolean_histograms)

        with self._measure_time("string_histograms_calc_time"):
            str_histograms = self._calculate_categorical_histograms_by_type(
                df, StringType, summary
            )
        histograms.update(str_histograms)

        with self._measure_time("numerical_histograms_calc_time"):
            numeric_histograms = self._calculate_numeric_histograms(df, summary)
        histograms.update(numeric_histograms)
        return histograms

    def _get_columns_by_type(self, dataframe, column_type):
        return [
            dataframe.select(f.name)
            for f in dataframe.schema.fields
            if isinstance(f.dataType, column_type)
        ]

    def _calculate_numeric_histograms(self, df, summary):
        numeric_columns = [
            f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)
        ]
        result = {column_name: None for column_name in numeric_columns}

        bucket_queries = []
        column_to_buckets = {column_name: [] for column_name in numeric_columns}
        for column_def in df.schema.fields:
            column_type, column_name = column_def.dataType, column_def.name
            if not isinstance(column_type, NumericType):
                continue

            column_buckets, column_bucket_queries = self._get_numerical_buckets(
                column_name, column_type, summary
            )
            bucket_queries.extend(column_bucket_queries)
            column_to_buckets[column_name] = column_buckets

        # For each 'bucket' (column with min/max values) we're performing
        # "select count(column) from table where column >= min and column < max"
        # then aggregating all values into single row with column aliases like "<column>_<bucket_number>"
        # note that we're performing batch histograms calculation only for numeric columns
        if len(bucket_queries):
            histograms = df.select(numeric_columns).agg(*bucket_queries).collect()[0]
        # Aggregating results into api-consumable form.
        # We're taking all buckets and looking up for counts for each bucket.
        for column in numeric_columns:
            counts = [
                histograms["%s_%s" % (column, i)]
                for i in range(0, len(column_to_buckets[column]) - 1)
            ]
            result[column] = (counts, column_to_buckets[column])

        return result

    def _get_numerical_buckets(self, column_name, column_type, summary):
        distinct = summary[column_name]["distinct"]
        minv = summary[column_name]["min"]
        maxv = summary[column_name]["max"]
        buckets_count = min(distinct, 20)
        if isinstance(column_type, IntegralType):
            # buckets calculation is copied from rdd.histogram() code and adjusted to be integers
            # for integral types bucket should be also integral that's why we're casting increment to int
            inc = int((maxv - minv) / buckets_count)
        else:
            inc = (maxv - minv) * 1.0 / buckets_count
        buckets = [i * inc + minv for i in range(buckets_count)]
        buckets.append(maxv)

        bucket_queries = []
        for i in range(0, len(buckets) - 1):
            bucket_queries.append(
                count(
                    when(
                        (buckets[i] <= col(column_name))
                        & (
                            col(column_name) <= buckets[i + 1]
                            if (i == len(buckets) - 2)
                            else col(column_name) < buckets[i + 1]
                        ),
                        1,
                    )
                ).alias("%s_%s" % (column_name, i))
            )
        return buckets, bucket_queries

    def _calculate_categorical_histograms_by_type(
        self, dataframe, column_type, summary
    ):
        max_buckets = 50

        column_df_list = self._get_columns_by_type(dataframe, column_type)
        if not column_df_list:
            return dict()

        value_counts = self._spark_categorical_histograms(column_df_list, max_buckets)
        histograms = self._convert_histogram_df_to_dict(value_counts)
        self._add_others(histograms, summary, max_buckets)
        return histograms

    def _spark_categorical_histograms(self, column_df_list, max_buckets):
        """ all columns in column_df_list should have the same type (e.g. all should be booleans or all strings) """
        value_counts = None
        for column_df in column_df_list:
            column_name = column_df.schema.names[0]
            column_value_counts = (
                column_df.groupby(column_name)
                .count()
                .orderBy(desc("count"))
                .withColumn("column_name", lit(column_name))
                .limit(max_buckets - 1)
            )

            if value_counts is None:
                value_counts = column_value_counts
            else:
                value_counts = value_counts.union(column_value_counts)
        return value_counts.collect()

    def _add_others(self, histograms, summary, max_buckets):
        """ sum all least significant values (who left out of histogram) to one bucket """
        for column_name, histogram in histograms.items():
            distinct = summary[column_name]["distinct"]
            if distinct < max_buckets:
                continue

            total_count = summary[column_name]["count"]
            histogram_sum_count = sum(histogram[0])
            others_count = total_count - histogram_sum_count
            histogram[0].append(others_count)
            histogram[1].append("_others")

    def _convert_histogram_df_to_dict(self, value_counts):
        histogram_dict = defaultdict(lambda: ([], []))
        for row in value_counts:
            value, count, column_name = row
            histogram_dict[column_name][0].append(count)
            histogram_dict[column_name][1].append(value)
        return histogram_dict

    @seven.contextlib.contextmanager
    def _measure_time(self, metric_key):
        try:
            start_time = time.time()
            yield
        finally:
            end_time = time.time()
            self.metrics[metric_key] = end_time - start_time
