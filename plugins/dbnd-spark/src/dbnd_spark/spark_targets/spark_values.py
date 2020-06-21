from __future__ import absolute_import

import logging
import typing

import pyspark.sql as spark

from pyspark.sql.functions import desc
from pyspark.sql.types import BooleanType, NumericType, StringType

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
    def _calculate_histogram(cls, column_df):
        total_count = column_df.count()
        distinct = column_df.distinct().count()
        column_df = column_df.na.drop()
        non_null = column_df.count()
        null_values = total_count - non_null
        stats_dict = {
            "count": non_null,
            "non-null": non_null,
            "null-count": null_values,
            "distinct": distinct,
        }

        column_name = column_df.schema.fields[0].name
        column_type = column_df.schema.fields[0].dataType
        histogram = None

        if isinstance(column_type, (StringType, BooleanType)):
            value_counts = (
                column_df.groupby(column_name).count().orderBy(desc("count")).collect()
            )
            counts = [row["count"] for row in value_counts]
            values = [row[column_name] for row in value_counts]

            if distinct > 50:
                counts, tail = counts[:49], counts[49:]
                counts.append(sum(tail))
                values = values[:49]
                values.append("_others")

            histogram = (counts, values)
        elif isinstance(column_type, NumericType):
            buckets = min(distinct, 20)
            histogram = column_df.rdd.map(lambda x: x[column_name]).histogram(buckets)
            histogram = tuple(reversed(histogram))

            stats_rows = column_df.summary().collect()
            summary = dict(
                [(row["summary"], float(row[column_name])) for row in stats_rows]
            )
            stats_dict.update(summary)
            stats_dict["std"] = stats_dict.pop("stddev")
        else:
            logger.info("Data type %s is not supported by histograms", column_type)

        return stats_dict, histogram

    @classmethod
    def get_histograms(cls, df):
        # type: (spark.DataFrame) -> Tuple[Optional[Dict[Dict[str, Any]]], Optional[Dict[str, Tuple]]]
        try:
            stats, histograms = dict(), dict()
            for column_name, column_type in df.dtypes:
                column = df.select(column_name)
                stats[column_name], histograms[column_name] = cls._calculate_histogram(
                    column
                )

            return stats, histograms
        except Exception:
            logger.exception("Error occured during histograms calculation")
            return None, None

    def support_fast_count(self, target):
        from targets import FileTarget

        if not isinstance(target, FileTarget):
            return False
        from targets.target_config import FileFormat

        return target.config.format == FileFormat.parquet
