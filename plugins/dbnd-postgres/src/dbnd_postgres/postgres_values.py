import time
import typing

import attr

from dbnd_postgres.postgres_controller import PostgresController
from targets.value_meta import ValueMeta
from targets.values import register_value_type
from targets.values.builtins_values import DataValueType


if typing.TYPE_CHECKING:
    from targets.value_meta import ValueMetaConf


@attr.s
class PostgresTable(object):
    table_name = attr.ib()  # type: str
    connection_string = attr.ib()  # type: str


class PostgresTableValueType(DataValueType):
    type = PostgresTable
    type_str = "PostgresTable"
    support_merge = False

    config_name = "postgres_table"

    def to_signature(self, value):
        # don't include user and password in uri
        db_uri = value.connection_string.split("@")[1]
        return db_uri + "/" + value.table_name

    def get_value_meta(self, value, meta_conf):
        # type: (PostgresTable, ValueMetaConf) -> ValueMeta
        data_schema = data_preview = None

        with PostgresController(value.connection_string, value.table_name) as postgres:
            if meta_conf.log_histograms or meta_conf.log_stats:
                start_time = time.time()
                columns_stats, histograms = postgres.get_histograms_and_stats(meta_conf)
                hist_sys_metrics = {
                    "histograms_and_stats_calc_time": time.time() - start_time
                }
            else:
                columns_stats, histograms = [], {}
                hist_sys_metrics = None
            if meta_conf.log_preview:
                data_preview = postgres.to_preview()
            if meta_conf.log_schema:
                data_schema = {"type": self.type_str, "dtypes": postgres.columns_types}

        return ValueMeta(
            value_preview=data_preview,
            data_dimensions=None,
            data_schema=data_schema,
            data_hash=self.to_signature(value),
            columns_stats=columns_stats,
            histogram_system_metrics=hist_sys_metrics,
            histograms=histograms,
        )


register_value_type(PostgresTableValueType())
