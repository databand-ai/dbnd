import logging

from textwrap import dedent
from typing import Dict

import attr
import snowflake.connector

from six.moves.urllib_parse import urlparse
from snowflake.connector import DictCursor

from dbnd import __version__
from dbnd._core.errors import DatabandConfigError, DatabandRuntimeError
from dbnd._core.utils.string_utils import humanize_bytes
from dbnd._vendor.tabulate import tabulate
from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values import register_value_type
from targets.values.builtins_values import DataValueType


logger = logging.getLogger(__name__)


class SnowflakeError(DatabandRuntimeError):
    pass


@attr.s
class SnowflakeTable(object):
    account = attr.ib()  # type: str
    user = attr.ib()  # type: str
    password = attr.ib()  # type: str
    database = attr.ib()  # type: str
    schema = attr.ib()  # type: str
    table_name = attr.ib()  # type: str
    preview_rows = attr.ib(default=20)  # type: int


class SnowflakeTableValueType(DataValueType):
    type = SnowflakeTable
    type_str = "SnowflakeTable"
    support_merge = False

    config_name = "snowflake_table"

    def get_snowflake(self, value):
        # TODO: Use sqlalchemy.engine.url here
        conn_string = SnowflakeController.conn_params_to_conn_str(
            account=value.account,
            user=value.user,
            password=value.password,
            database=value.database,
            schema=value.schema,
        )
        return SnowflakeController(conn_string)

    def to_signature(self, x):
        # type: (SnowflakeTable) -> str
        # don't include user and password in uri
        return "snowflake://{0.user}:***@{0.account}/{0.database}.{0.schema}/{0.table_name}".format(
            x
        )

    def get_value_meta(self, value, meta_conf):
        # type: (SnowflakeTable, ValueMetaConf) -> ValueMeta
        data_schema = {}
        data_preview = data_dimensions = None

        with self.get_snowflake(value) as snowflake:
            stats, histograms = {}, {}
            hist_sys_metrics = None
            if meta_conf.log_preview:
                data_preview = snowflake.to_preview(value)
            if meta_conf.log_schema:
                data_schema = {
                    "type": self.type_str,
                    "column_types": snowflake.get_column_types(value),
                }
            if meta_conf.log_size:
                dimensions = snowflake.get_dimensions(value)
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

# TODO? Subclass from SingletonContext to reuse connection??
class SnowflakeController:
    """ Interacts with Snowflake, queries it"""

    def __init__(self, connection_string):
        conn_params = conn_str_to_conn_params(connection_string)

        self.connection_string = connection_string
        self.account = conn_params["account"]
        self.user = conn_params["user"]
        self.password = conn_params["password"]

        self._query_tag = "dbnd-snowflake"
        self._connection = None
        self._column_types = None

    @staticmethod
    def conn_params_to_conn_str(**conn_params):
        # type: (**Dict) -> str
        # TODO: Drop in favor of sqlalchemy.engine.url
        uri = "snowflake://{user}:{password}@{account}/"
        return uri.format(**conn_params)

    def __enter__(self):
        self._connection = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            session_parameters={"QUERY_TAG": self._query_tag},
            application="DBND Snowflake plugin {}".format(__version__),
        )
        self._cursor = self._connection.cursor(DictCursor)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection is not None:
            self._connection.close()

    def to_preview(self, table):
        # type: (SnowflakeTable) -> str
        # snowflake.connector does not handle incorrect utf-8 data fetched from db,
        # hence this fillding with encode/decode
        column_types = self.get_column_types(table)
        columns = ",".join(
            'TRY_HEX_DECODE_STRING(HEX_ENCODE("{0}")) AS {0}'.format(column)
            for column in column_types.keys()
        )

        rows = self._query(
            "select {0} from {1.database}.{1.schema}.{1.table_name} limit {1.preview_rows}".format(
                columns, table
            )
        )
        preview_table = tabulate(rows, headers="keys") + "\n..."
        return preview_table

    def get_dimensions(self, table):
        # type: (SnowflakeTable) -> Dict
        table_meta = self._query(
            "SHOW TABLES LIKE '{0.table_name}' in schema {0.database}.{0.schema}".format(
                table
            )
        )
        if len(table_meta) != 1:
            raise SnowflakeError(
                "Snowflake table not found: '{0.table_name}', DB: '{0.database}'".format(
                    table
                )
            )

        cols = len(self.get_column_types(table))
        return {
            "rows": table_meta[0]["rows"],
            "cols": cols,
            "bytes": table_meta[0]["bytes"],
        }

    def get_column_types(self, table):
        # type: (SnowflakeTable) -> Dict[str, str]
        if self._column_types is not None:
            return self._column_types

        query = dedent(
            """\
            SELECT column_name, data_type
            FROM {0.database}.information_schema.columns
            WHERE LOWER(table_name) = LOWER('{0.table_name}')
                and LOWER(table_schema) = LOWER('{0.schema}')"""
        ).format(table)
        results = self._query(query)
        if not results:
            raise SnowflakeError(
                "Table columns not found. Snowflake DB: '{0.database}', "
                "schema: {0.schema} table: '{0.table_name}'\n"
                "Query used: {1}".format(table, query)
            )

        self._column_types = {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in results}
        return self._column_types

    def _query(self, query, params=None):
        try:
            self._cursor.execute(query, params)
            result = self._cursor.fetchall()
            return result
        except Exception:
            logger.exception(
                "Error occurred during querying Snowflake, query: %s", query
            )
            raise


def conn_str_to_conn_params(conn_str):
    # type: (str) -> dict
    # TODO: Drop in favor of sqlalchemy.engine.url
    conn = urlparse(conn_str)
    if conn.scheme != "snowflake":
        raise DatabandConfigError(
            "Unsupported connection string scheme '{}'. snowflake is required".format(
                conn.scheme
            )
        )

    return {
        "account": conn.hostname,
        "user": conn.username,
        "password": conn.password,
    }
