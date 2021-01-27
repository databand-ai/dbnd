import logging
import typing

import attr

from dbnd._core.utils.string_utils import humanize_bytes
from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values import register_value_type
from targets.values.builtins_values import DataValueType


if typing.TYPE_CHECKING:
    from dbnd_snowflake.snowflake_controller import SnowflakeController


logger = logging.getLogger(__name__)


@attr.s
class SnowflakeTable(object):
    snowflake_ctrl = attr.ib()  # type: SnowflakeController
    database = attr.ib()  # type: str
    schema = attr.ib()  # type: str
    table_name = attr.ib()  # type: str
    preview_rows = attr.ib(default=20)  # type: int

    @property
    def db_with_schema(self):
        if self.schema:
            return "{0}.{1}".format(self.database, self.schema)
        return self.database


class SnowflakeTableValueType(DataValueType):
    type = SnowflakeTable
    type_str = "SnowflakeTable"
    support_merge = False

    config_name = "snowflake_table"

    def to_signature(self, x):
        # type: (SnowflakeTable) -> str
        return "{0.snowflake_ctrl}/{0.db_with_schema}/{0.table_name}".format(x)

    def get_value_meta(self, value, meta_conf):
        # type: (SnowflakeTable, ValueMetaConf) -> ValueMeta
        data_schema = {}
        data_preview = data_dimensions = None

        stats, histograms = {}, {}
        hist_sys_metrics = None
        if meta_conf.log_preview:
            data_preview = value.snowflake_ctrl.to_preview(value)
        if meta_conf.log_schema:
            data_schema = {
                "type": self.type_str,
                "column_types": value.snowflake_ctrl.get_column_types(value),
            }
        if meta_conf.log_size:
            dimensions = value.snowflake_ctrl.get_dimensions(value)
            data_dimensions = [dimensions["rows"], dimensions["cols"]]
            data_schema["size"] = humanize_bytes(dimensions["bytes"])

        return ValueMeta(
            value_preview=data_preview,
            data_dimensions=data_dimensions,
            data_schema=data_schema,
            data_hash=self.to_signature(value),
            descriptive_stats=stats,
            histogram_system_metrics=hist_sys_metrics,
            histograms=histograms,
        )


register_value_type(SnowflakeTableValueType())
