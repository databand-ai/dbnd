from __future__ import absolute_import

import logging
import typing

import pyspark.sql as spark

from pyspark.sql.types import BooleanType, NumericType, StringType

from dbnd._core.tracking.histograms import HistogramDataType
from dbnd_spark.spark_targets.spark_histograms import SparkHistograms
from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values.builtins_values import DataValueType


if typing.TYPE_CHECKING:
    from typing import Dict

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

    @staticmethod
    def __types_map(dataType):
        if isinstance(dataType, BooleanType):
            return HistogramDataType.boolean
        elif isinstance(dataType, NumericType):
            return HistogramDataType.numeric
        elif isinstance(dataType, StringType):
            return HistogramDataType.string
        else:
            return HistogramDataType.other

    def get_all_data_columns(self, df):
        # type: (spark.DataFrame) -> Dict[str, HistogramDataType]
        return {f.name: self.__types_map(f.dataType) for f in df.schema.fields}

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

        if meta_conf.log_histograms:
            histogram_spec = meta_conf.get_histogram_spec(self, value)
            spark_histograms = SparkHistograms(histogram_spec)
            df_stats, histogram_dict = spark_histograms.get_histograms(value)
            hist_sys_metrics = spark_histograms.metrics
        else:
            df_stats, histogram_dict = {}, {}
            histogram_spec = hist_sys_metrics = None

        return ValueMeta(
            value_preview=data_preview,
            data_dimensions=data_dimensions,
            data_schema=data_schema,
            data_hash=self.to_signature(value),
            descriptive_stats=df_stats,
            histogram_spec=histogram_spec,
            histogram_system_metrics=hist_sys_metrics,
            histograms=histogram_dict,
        )

    def support_fast_count(self, target):
        from targets import FileTarget

        if not isinstance(target, FileTarget):
            return False
        from targets.target_config import FileFormat

        return target.config.format == FileFormat.parquet
