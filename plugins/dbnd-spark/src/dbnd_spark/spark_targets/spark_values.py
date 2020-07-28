from __future__ import absolute_import

import logging
import typing

import pyspark.sql as spark

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    desc,
    isnull,
    mean,
    stddev,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    IntegralType,
    MapType,
    NumericType,
    StringType,
    StructType,
)

from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values.builtins_values import DataValueType


if typing.TYPE_CHECKING:
    from typing import Tuple, Optional, Dict, Any


logger = logging.getLogger(__name__)


class SparkDataFrameValueType(DataValueType):
    type = spark.DataFrame
    type_str = "Spark.DataFrame"
    support_merge = False

    config_name = "spark_dataframe"

    def to_signature(self, x):
        id = "rdd-%s-at-%s" % (x.rdd.id(), x.rdd.context.applicationId)
        return id

    def to_preview(self, df, preview_size):  # type: (spark.DataFrame, int) -> str
        return (
            df.limit(1000)
            .toPandas()
            .to_string(index=False, max_rows=20, max_cols=1000)[:preview_size]
        )

    def get_value_meta(self, value, meta_conf):
        # type: (spark.DataFrame, ValueMetaConf) -> ValueMeta

        if meta_conf.log_schema:
            data_schema = {
                "type": self.type_str,
                "columns": list(value.schema.names),
                "dtypes": {f.name: str(f.dataType) for f in value.schema.fields},
            }
        else:
            data_schema = None

        if meta_conf.log_preview:
            data_preview = self.to_preview(value, meta_conf.get_preview_size())
        else:
            data_preview = None

        if meta_conf.log_stats:
            data_schema["stats"] = self.to_preview(
                value.summary(), meta_conf.get_preview_size()
            )

        if meta_conf.log_size:
            data_schema = data_schema or {}
            rows = value.count()
            data_dimensions = (rows, len(value.columns))
            data_schema.update(
                {
                    "size": int(rows * len(value.columns)),
                    "shape": (rows, len(value.columns)),
                }
            )
        else:
            data_dimensions = None

        df_stats, histograms = None, None
        if meta_conf.log_df_hist:
            df_stats, histograms = self.get_histograms(value)

        return ValueMeta(
            value_preview=data_preview,
            data_dimensions=data_dimensions,
            data_schema=data_schema,
            data_hash=self.to_signature(value),
            descriptive_stats=df_stats,
            histograms=histograms,
        )

    @classmethod
    def get_histograms(cls, df):
        # type: (spark.DataFrame) -> Tuple[Optional[Dict[Dict[str, Any]]], Optional[Dict[str, Tuple]]]
        try:
            df = cls._filter_complex_columns(df)

            summary = cls._calculate_summary(df)
            histograms = cls._calculate_histograms(df, summary)

            return summary, histograms
        except Exception:
            logger.exception("Error occured during histograms calculation")
            return None, None

    @classmethod
    def _filter_complex_columns(cls, df):
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

    @classmethod
    def _calculate_summary(cls, df):
        summary = df.summary().collect()
        # non-null and distinct counts should be calculated in separate query, because summary doesn't includes it
        expressions = (
            [countDistinct(col(c)).alias("%s_distinct" % c) for c in df.columns]
            + [count(when(isnull(c), c)).alias("%s_count_null" % c) for c in df.columns]
            + [count(col(c)).alias("%s_count" % c) for c in df.columns]
        )
        counts = df.agg(*expressions).collect()[0]

        result = {column_name: {} for column_name in df.columns}
        for column_def in df.schema.fields:
            column_name = column_def.name
            if isinstance(column_def.dataType, NumericType):
                # summary (min, max, mean, std, etc.) make sense only for numeric types
                for row in summary:
                    # zero cell contains summary metrics name
                    result[column_name][row[0]] = float(row[column_name])
                # so frontend will be able to eat metric
                result[column_name]["std"] = result[column_name]["stddev"]
            result[column_name]["distinct"] = counts["%s_distinct" % column_name]
            result[column_name]["null-count"] = counts["%s_count_null" % column_name]
            result[column_name]["non-null"] = counts["%s_count" % column_name]
            # count in summary calculates only non-null counts, so we have to summarize non-null and null
            result[column_name]["count"] = (
                result[column_name]["non-null"] + result[column_name]["null-count"]
            )
        return result

    @classmethod
    def _calculate_histograms(cls, df, summary):
        result = {column_name: None for column_name in df.columns}

        # all_buckets contains tuples of:
        # - column name
        # - bucket min value
        # - bucket max value
        # - bucket number
        # we need such bunch of tuples to perform single spark query later
        all_buckets = []

        numeric_columns = [
            f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)
        ]
        # column -> [buckets]
        named_buckets = {column_name: [] for column_name in numeric_columns}

        for column_def in df.schema.fields:
            column_type = column_def.dataType
            column_name = column_def.name
            distinct = summary[column_name]["distinct"]
            # string values histograms are calculated in old N+1 way
            if isinstance(column_type, (StringType, BooleanType)):
                value_counts = (
                    df.select(column_name)
                    .groupby(column_name)
                    .count()
                    .orderBy(desc("count"))
                    .collect()
                )
                counts = [row["count"] for row in value_counts]
                values = [row[column_name] for row in value_counts]

                if distinct > 50:
                    counts, tail = counts[:49], counts[49:]
                    counts.append(sum(tail))
                    values = values[:49]
                    values.append("_others")

                result[column_name] = (counts, values)
            elif isinstance(column_type, NumericType):
                minv = summary[column_name]["min"]
                maxv = summary[column_name]["max"]

                # buckets count should be 20 or number of distinct elements in the column
                buckets_count = min(distinct, 20)

                if isinstance(column_type, IntegralType):
                    # buckets calculation is copied from rdd.histogram() code and ajusted to be integers
                    # for integral types bucket should be also integral that's why we're casting increment to int
                    inc = int((maxv - minv) / buckets_count)
                else:
                    inc = (maxv - minv) * 1.0 / buckets_count

                buckets = [i * inc + minv for i in range(buckets_count)]
                buckets.append(maxv)

                for i in range(0, len(buckets) - 1):
                    all_buckets.append(
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
                named_buckets[column_name] = buckets
            else:
                logger.info("Data type %s is not supported by histograms", column_type)

        # For each 'bucket' (column with min/max values) we're performing
        # "select count(column) from table where column >= min and column < max"
        # then aggregating all values into single row with column aliases like "<column>_<bucket_number>"
        # note that we're performing batch histograms calculation only for numeric columns
        if len(all_buckets):
            histograms = df.select(numeric_columns).agg(*all_buckets).collect()[0]

        # Aggregating results into api-consumable form.
        # We're taking all buckets and looking up for counts for each bucket.
        for column in numeric_columns:
            counts = [
                histograms["%s_%s" % (column, i)]
                for i in range(0, len(named_buckets[column]) - 1)
            ]
            result[column] = (counts, named_buckets[column])

        return result

    def support_fast_count(self, target):
        from targets import FileTarget

        if not isinstance(target, FileTarget):
            return False
        from targets.target_config import FileFormat

        return target.config.format == FileFormat.parquet
