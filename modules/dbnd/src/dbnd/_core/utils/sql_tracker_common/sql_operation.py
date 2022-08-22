# © Copyright Databand.ai, an IBM Company 2022

from collections import defaultdict
from itertools import chain
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import attr

from more_itertools import first, padded

from dbnd._core.constants import DbndDatasetOperationType, DbndTargetOperationType
from dbnd._core.utils.sql_tracker_common.sql_extract import Column, Schema
from dbnd._core.utils.sql_tracker_common.utils import strip_quotes
from dbnd.utils.anonymization import secrets_anonymizer
from targets.connections import build_conn_path
from targets.value_meta import ValueMeta
from targets.values import register_value_type
from targets.values.builtins_values import DataValueType
from targets.values.pandas_histograms import PandasHistograms


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
    error: str = attr.ib()
    dataframe = attr.ib(default=None)  # type: pd.DataFrame

    def __attrs_post_init__(self):
        self.query = secrets_anonymizer.anonymize(self.query)

    @property
    def columns(self) -> List[str]:
        try:
            return list(self.dtypes)
        except TypeError:
            return None

    @property
    def columns_count(self) -> int:
        try:
            return len(self.dtypes)
        except TypeError:
            return None

    @property
    def is_file(self) -> bool:
        # files are in scope of read operations
        return (
            self.op_type is DbndTargetOperationType.read
            and first(self.extracted_columns).is_file
        )

    @property
    def is_stage(self) -> bool:
        # stage are in scope of read operations
        return (
            self.op_type is DbndTargetOperationType.read
            and first(self.extracted_columns).is_stage
        )

    @property
    def tables(self) -> Iterable[str]:
        return [col.dataset_name for col in self.extracted_columns]

    @property
    def extracted_columns(self) -> Iterable[Column]:
        return chain.from_iterable(self.extracted_schema.values())

    def evolve_schema(
        self, tables_schemas: Dict[str, DTypes], file_schema: DTypes = None
    ) -> "SqlOperation":
        if self.dtypes is not None:
            return self

        dtypes = self.build_dtypes(tables_schemas, file_schema)
        return attr.evolve(self, dtypes=dtypes)

    def build_dtypes(
        self, tables_schemas: Dict[str, DTypes], file_schema: DTypes = None
    ) -> DTypes:
        dtypes = {}
        for col in self.extracted_columns:
            if col.is_file or col.is_stage:
                if file_schema:
                    dtypes.update(file_schema)
                else:
                    continue
            if col.is_wildcard:
                all_columns = tables_schemas.get(col.dataset_name, {})
                dtypes.update(all_columns)
            else:
                col_name = col.name.strip('"')
                col_type = tables_schemas.get(col.dataset_name, {}).get(
                    col_name.lower()
                )
                if col_type:
                    dtypes[col_name] = col_type
        return dtypes if dtypes else None

    def evolve_table_name(self, connection: Connection) -> "SqlOperation":
        schema: Schema = defaultdict(list)
        for name, cols in self.extracted_schema.items():
            for col in cols:
                if not col.is_file:
                    table_name = render_table_name(connection, col.dataset_name)
                else:
                    table_name = col.dataset_name
                col = attr.evolve(col, dataset_name=table_name)
                schema[name].append(col)

        return attr.evolve(self, extracted_schema=schema)


def split_table_name(table_name: str) -> Tuple[str, Optional[str], Optional[str]]:
    table, schema, database = padded(reversed(table_name.split(".")[:3]), None, 3)
    return database, schema, table


def render_table_name(connection: Connection, table_name: str, sep: str = ".") -> str:
    database, schema, table = split_table_name(table_name)
    database = connection.database or database
    schema = connection.schema or schema
    path = sep.join(map(strip_quotes, filter(None, [database, schema, table])))
    return path


def render_connection_path(
    connection: Connection, operation: SqlOperation, conn_type: str
) -> str:
    # We takes only the first table as a workaround
    table_name = first(operation.tables)
    if operation.is_file:
        urlparsed = urlparse(table_name)
        return build_conn_path(
            conn_type=urlparsed.scheme,
            hostname=urlparsed.netloc,
            port=urlparsed.port,
            path=urlparsed.path,
        )
    else:
        path = render_table_name(connection, table_name, sep="/")
    return build_conn_path(
        conn_type=conn_type, hostname=connection.host, port=connection.port, path=path
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
            data_schema = {"type": self.type_str, "dtypes": value.dtypes}

        if meta_conf.log_size:
            data_dimensions = [value.records_count, value.columns_count]
            data_schema["shape"] = data_dimensions
            # todo: size?

        # currently columns_stats and histogram are not supported
        columns_stats, histograms = [], {}
        hist_sys_metrics = None
        if meta_conf.log_stats and value.dataframe is not None:
            columns_stats, _ = PandasHistograms(
                value.dataframe, meta_conf
            ).get_histograms_and_stats()

        return ValueMeta(
            value_preview=None,
            data_dimensions=data_dimensions,
            query=value.query,
            data_schema=data_schema,
            data_hash=str(hash(self.to_signature(value))),
            columns_stats=columns_stats,
            histogram_system_metrics=hist_sys_metrics,
            histograms=histograms,
        )


register_value_type(SqlOperationValueMeta())
