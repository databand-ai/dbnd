from collections import defaultdict
from itertools import chain
from typing import Dict, Iterable, List, Optional, Tuple

import attr

from more_itertools import first, padded

from dbnd._core.constants import DbndDatasetOperationType
from dbnd_snowflake.POC.sql_extract import Column, Schema
from targets.connections import build_conn_path
from targets.value_meta import ValueMeta
from targets.values import register_value_type
from targets.values.builtins_values import DataValueType


class Connection:
    @property
    def host(self) -> str:
        ...

    @property
    def port(self) -> Optional[int]:
        ...

    @property
    def database(self) -> Optional[str]:
        ...

    @property
    def schema(self) -> Optional[str]:
        ...


DTypes = Dict[str, str]


@attr.s
class SqlOperation:
    extracted_schema: Schema = attr.ib()
    dtypes: DTypes = attr.ib()
    records_count: int = attr.ib()
    query: str = attr.ib()
    query_id: str = attr.ib()
    success: bool = attr.ib()
    op_type: DbndDatasetOperationType = attr.ib()

    @property
    def columns(self) -> Optional[List[str]]:
        try:
            return list(self.dtypes)
        except TypeError:
            return None

    @property
    def columns_count(self) -> Optional[int]:
        try:
            return len(self.dtypes)
        except TypeError:
            return None

    @property
    def tables(self) -> Iterable[str]:
        return [col.table for col in self.extracted_columns]

    @property
    def extracted_columns(self) -> Iterable[Column]:
        return chain.from_iterable(self.extracted_schema.values())

    def evolve_schema(self, tables_schemas: Dict[str, DTypes]) -> "SqlOperation":
        if self.dtypes is not None:
            return self

        dtypes = self.build_dtypes(tables_schemas)
        return attr.evolve(self, dtypes=dtypes)

    def build_dtypes(self, tables_schemas: Dict[str, DTypes]) -> DTypes:
        dtypes = {}
        for col in self.extracted_columns:
            if col.is_wildcard:
                all_columns = tables_schemas[col.table]
                dtypes.update(all_columns)
            else:
                col_type = tables_schemas[col.table][col.name.lower()]
                dtypes[col.name] = col_type
        return dtypes

    def evolve_table_name(self, connection: Connection) -> "SqlOperation":
        schema: Schema = defaultdict(list)
        for name, cols in self.extracted_schema.items():
            for col in cols:
                table_name = render_table_name(connection, col.table)
                col = attr.evolve(col, table=table_name)
                schema[name].append(col)

        return attr.evolve(self, extracted_schema=schema)


def split_table_name(table_name: str) -> Tuple[str, Optional[str], Optional[str]]:
    table, schema, database = padded(reversed(table_name.split(".")[:3]), None, 3)
    return database, schema, table


def render_table_name(connection: Connection, table_name: str, sep: str = ".") -> str:
    database, schema, table = split_table_name(table_name)

    database = database or connection.database
    schema = schema or connection.schema

    path = sep.join(filter(None, [database, schema, table],))
    return path


def render_connection_path(
    connection: Connection, operation: SqlOperation, conn_type: str
) -> str:
    # We takes only the first table as a workaround
    table_name = first(operation.tables)
    path = render_table_name(connection, table_name, sep="/")
    return build_conn_path(
        conn_type=conn_type, hostname=connection.host, port=connection.port, path=path,
    )


class SqlOperationValueMeta(DataValueType):
    type = SqlOperation
    type_str = "SqlOperation"
    support_merge = False

    config_name = "sql_operation"
    is_lazy_evaluated = False

    def get_value_meta(self, value: SqlOperation, meta_conf):
        data_schema = {}
        data_dimensions = None

        if meta_conf.log_schema:
            data_schema = {
                "type": self.type_str,
                "dtypes": value.dtypes,
            }

        if meta_conf.log_size:
            data_dimensions = [value.records_count, value.columns_count]
            data_schema["shape"] = data_dimensions
            # todo: size?

        # currently stats and histogram are not supported
        stats, histograms = {}, {}
        hist_sys_metrics = None

        return ValueMeta(
            value_preview=None,
            data_dimensions=data_dimensions,
            data_schema=data_schema,
            data_hash=str(hash(self.to_signature(value))),
            descriptive_stats=stats,
            histogram_system_metrics=hist_sys_metrics,
            histograms=histograms,
        )


register_value_type(SqlOperationValueMeta())
