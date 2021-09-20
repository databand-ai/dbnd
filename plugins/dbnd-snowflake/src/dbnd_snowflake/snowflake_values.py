import logging
import typing

from typing import Optional

import attr

from dbnd_snowflake.snowflake_config import SnowflakeConfig
from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values import register_value_type
from targets.values.builtins_values import DataValueType


if typing.TYPE_CHECKING:
    from dbnd_snowflake.snowflake_controller import SnowflakeController

logger = logging.getLogger(__name__)


def _strip_quotes(v):
    # type: (str) -> str
    return v.strip('"') if v else v


@attr.s
class SnowflakeTable(object):
    snowflake_ctrl = attr.ib()  # type: SnowflakeController
    database = attr.ib(converter=_strip_quotes)  # type: str
    schema = attr.ib(converter=_strip_quotes)  # type: str
    table_name = attr.ib(converter=_strip_quotes)  # type: str
    preview_rows = attr.ib(default=20)  # type: int

    @property
    def db_with_schema(self):
        if self.schema:
            return "{0}.{1}".format(self.database, self.schema)
        return self.database

    @property
    def database_q(self):
        return '"{}"'.format(self.database) if self.database else self.database

    @property
    def schema_q(self):
        return '"{}"'.format(self.schema) if self.schema else self.schema

    @property
    def table_name_q(self):
        return '"{}"'.format(self.table_name) if self.table_name else self.table_name

    @property
    def db_with_schema_q(self):
        if self.schema:
            return "{0}.{1}".format(self.database_q, self.schema_q)
        return self.database

    def __str__(self):
        return "{0}.{1}".format(self.db_with_schema, self.table_name)

    @classmethod
    def from_table(cls, snowflake_ctrl, table, config=None):
        # type: (SnowflakeController, str, Optional[SnowflakeConfig]) -> SnowflakeTable
        if config is None:
            config = SnowflakeConfig.current()
        preview_rows = config.table_preview_rows

        # this case is very simple, we need to extract from the controller if there is a missing part
        rest, _, table_name = table.rpartition(".")
        database, _, schema = rest.rpartition(".")

        return cls(snowflake_ctrl, database, schema, table_name, preview_rows)


class SnowflakeTableValueType(DataValueType):
    type = SnowflakeTable
    type_str = "SnowflakeTable"
    support_merge = False

    config_name = "snowflake_table"

    is_lazy_evaluated = True

    def to_signature(self, x):
        # type: (SnowflakeTable) -> str
        return "{0.snowflake_ctrl}/{0.db_with_schema}/{0.table_name}".format(x)

    def get_value_meta(self, value, meta_conf):
        # type: (SnowflakeTable, ValueMetaConf) -> ValueMeta
        data_schema = {}
        data_preview = data_dimensions = None
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
            data_schema["size.bytes"] = dimensions["bytes"]

        # currently stats and histogram are not supported
        stats, histograms = {}, {}
        hist_sys_metrics = None

        return ValueMeta(
            value_preview=data_preview,
            data_dimensions=data_dimensions,
            data_schema=data_schema,
            data_hash=str(hash(self.to_signature(value))),
            descriptive_stats=stats,
            histogram_system_metrics=hist_sys_metrics,
            histograms=histograms,
        )


register_value_type(SnowflakeTableValueType())
