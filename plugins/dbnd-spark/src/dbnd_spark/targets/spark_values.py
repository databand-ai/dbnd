from __future__ import absolute_import

from typing import Tuple

import pyspark.sql as spark

from dbnd._core.utils import json_utils
from targets.config import get_value_preview_max_len
from targets.values.builtins_values import DataValueType


class SparkDataFrameValueType(DataValueType):
    type = spark.DataFrame
    type_str = "Spark.DataFrame"
    support_merge = False

    config_name = "spark_dataframe"

    def to_signature(self, x):
        return str(x.rdd.id())

    def to_preview(self, df):  # type: (spark.DataFrame) -> str
        return (
            df.limit(1000)
            .toPandas()
            .to_string(index=False, max_rows=20, max_cols=1000)[
                : get_value_preview_max_len()
            ]
        )

    def get_data_dimensions(self, value):  # type: (spark.DataFrame) -> Tuple[int]
        return value.count(), len(value.columns)

    def get_data_schema(self, df):  # type: (spark.DataFrame) -> str

        schema = {
            "type": self.type_str,
            "columns": list(df.schema.names),
            "size": int(df.count() * len(df.columns)),
            "shape": (df.count(), len(df.columns)),
            "dtypes": {f.name: str(f.dataType) for f in df.schema.fields},
        }
        return json_utils.dumps(schema)

    def get_data_hash(self, value):
        # TODO Hash dataframe path
        return None
