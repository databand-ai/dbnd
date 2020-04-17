from __future__ import absolute_import

from typing import Optional

import pyspark.sql as spark

from dbnd._core.utils import json_utils
from targets.config import get_value_preview_max_len
from targets.target_meta import TargetMeta
from targets.values.builtins_values import DataValueType


class SparkDataFrameValueType(DataValueType):
    type = spark.DataFrame
    type_str = "Spark.DataFrame"
    support_merge = False

    config_name = "spark_dataframe"

    def to_signature(self, x):
        id = "rdd-%s-at-%s" % (x.rdd.id(), x.rdd.context.applicationId)
        return id

    def to_preview(self, df):  # type: (spark.DataFrame) -> str
        return (
            df.limit(1000)
            .toPandas()
            .to_string(index=False, max_rows=20, max_cols=1000)[
                : get_value_preview_max_len()
            ]
        )

    def get_value_meta(self, value, with_preview=True):
        # type: (spark.DataFrame, Optional[bool]) -> TargetMeta

        data_dimensions = None
        data_schema = {
            "type": self.type_str,
            "columns": list(value.schema.names),
            "dtypes": {f.name: str(f.dataType) for f in value.schema.fields},
        }

        if with_preview:
            rows = value.count()
            data_dimensions = (rows, len(value.columns))
            data_schema.update(
                {
                    "size": int(rows * len(value.columns)),
                    "shape": (rows, len(value.columns)),
                }
            )

        return TargetMeta(
            value_preview=self.to_preview(value) if with_preview else None,
            data_dimensions=data_dimensions,
            data_schema=json_utils.dumps(data_schema),
            data_hash=self.to_signature(value),
        )
