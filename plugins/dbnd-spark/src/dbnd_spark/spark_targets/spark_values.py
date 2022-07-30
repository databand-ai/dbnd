# © Copyright Databand.ai, an IBM Company 2022

from __future__ import absolute_import

import logging
import typing

from collections import defaultdict
from typing import List

import pyspark.sql as spark

from dbnd._core.tracking.schemas.column_stats import ColumnStatsArgs
from targets.value_meta import ValueMeta
from targets.values.builtins_values import DataValueType


LOG_DATASET_OP_PYSPARK_SOURCE = "pyspark_manual_logging"

if typing.TYPE_CHECKING:
    from targets.value_meta import ValueMetaConf

logger = logging.getLogger(__name__)

"""
    helper function to preview pyspark dataframe as string by using inner function _jdf.showString
    :param DataFrame df
            dataframe to preview
    :param int n, optional
            number of rows to preview
    :param truncate : bool or int, optional
            If set to ``True``, truncate strings longer than 20 chars by default.
            If set to a number greater than one, truncates long strings to length ``truncate``
            and align cells right.
     :return: str
     """


def get_show_string(df, n=20, truncate=True):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 20, False)
    else:
        return df._jdf.showString(n, int(truncate), False)


class SparkDataFrameValueType(DataValueType):
    type = spark.DataFrame
    type_str = "Spark.DataFrame"
    support_merge = False

    config_name = "spark_dataframe"

    is_lazy_evaluated = True

    def to_signature(self, x):
        id = "rdd-%s-at-%s" % (x.rdd.id(), x.rdd.context.applicationId)
        return id

    def to_preview(self, df, preview_size):  # type: (spark.DataFrame, int) -> str
        preview_alias = df.alias("DBND_INTERNAL_PREVIEW")
        try:
            return get_show_string(df=preview_alias.limit(1000), truncate=True)
        except Exception:
            logger.exception("Unexpected error, dataframe preview cannot be shown")

    def get_value_meta(self, value, meta_conf):
        # type: (spark.DataFrame, ValueMetaConf) -> ValueMeta
        if meta_conf.log_schema:
            schema_alias = value.alias("DBND_INTERNAL_SCHEMA")
            data_schema = {
                "type": self.type_str,
                "columns": list(schema_alias.schema.names),
                "dtypes": {f.name: str(f.dataType) for f in schema_alias.schema.fields},
            }
        else:
            data_schema = None

        if meta_conf.log_preview:
            data_preview = self.to_preview(value, meta_conf.get_preview_size())
        else:
            data_preview = None

        if meta_conf.log_size:
            size_alias = value.alias("DBND_INTERNAL_DIMS")
            data_schema = data_schema or {}
            rows = size_alias.count()
            data_dimensions = (rows, len(size_alias.columns))
            data_schema.update(
                {
                    "size.bytes": int(rows * len(size_alias.columns)),
                    "shape": (rows, len(size_alias.columns)),
                }
            )
        else:
            data_dimensions = None

        df_columns_stats, histogram_dict, hist_sys_metrics = [], {}, {}

        if meta_conf.log_histograms:
            logger.warning("log_histograms are not supported for spark dataframe")

        if meta_conf.log_stats:
            df_columns_stats = self.calculate_spark_stats(value)

        return ValueMeta(
            value_preview=data_preview,
            data_dimensions=data_dimensions,
            data_schema=data_schema,
            data_hash=self.to_signature(value),
            columns_stats=df_columns_stats,
            histogram_system_metrics=hist_sys_metrics,
            histograms=histogram_dict,
            op_source=LOG_DATASET_OP_PYSPARK_SOURCE,
        )

    def calculate_spark_stats(
        self, df
    ):  # type: (spark.DataFrame) -> List[ColumnStatsArgs]
        """
        Calculate descriptive statistics for Spark Dataframe and return them in format consumable by tracker.
        Spark has built-in method for stats calculation, it returns table like this:
        +-------+-------------+--------------------+
        |summary|serial_number|      capacity_bytes|
        +-------+-------------+--------------------+
        |  count|        42390|               42389|
        |   mean|         null|2.549704589668174E12|
        | stddev|         null|7.415846913745422E11|
        |    min|     5XW004Q0|       1000204886016|
        |    25%|         null|       2000398934016|
        |    50%|         null|       3000592982016|
        |    75%|         null|       3000592982016|
        |    max|     Z2926ALH|       4000787030016|
        +-------+-------------+--------------------+
        Each row represents specific metric values for every column.
        We are iterating over this table and converting results to list of ColumnStatsArgs
        :param df:
        :return:
        """
        stats_alias = df.alias("DBND_INTERNAL_STATS")
        total_count = stats_alias.count()
        stats = defaultdict(dict)

        for row in stats_alias.summary().collect():
            metric_row = row.asDict()
            metric_name = metric_row["summary"]
            for col in stats_alias.columns:
                stats[col][metric_name] = metric_row.get(col)

        result: List[ColumnStatsArgs] = []
        for col in stats_alias.schema.fields:
            if not isinstance(
                col.dataType, (spark.types.NumericType, spark.types.StringType)
            ):
                # We calculate descriptive statistics only for numeric and string columns
                continue

            name = col.name
            col_stats = stats[name]
            if isinstance(col.dataType, spark.types.StringType):
                result.append(
                    ColumnStatsArgs(
                        column_name=name,
                        column_type=str(col.dataType),
                        records_count=total_count,
                        null_count=total_count - int(col_stats["count"]),
                    )
                )
            elif isinstance(col.dataType, spark.types.NumericType):
                result.append(
                    ColumnStatsArgs(
                        column_name=name,
                        column_type=str(col.dataType),
                        records_count=total_count,
                        null_count=total_count - int(col_stats["count"]),
                        min_value=col_stats["min"],
                        max_value=col_stats["max"],
                        std_value=col_stats["stddev"],
                        mean_value=col_stats["mean"],
                        quartile_1=col_stats["25%"],
                        quartile_2=col_stats["50%"],
                        quartile_3=col_stats["75%"],
                    )
                )

        return result

    def support_fast_count(self, target):
        from targets import FileTarget

        if not isinstance(target, FileTarget):
            return False
        from targets.target_config import FileFormat

        return target.config.format == FileFormat.parquet
