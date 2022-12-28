# © Copyright Databand.ai, an IBM Company 2022

import logging

from itertools import takewhile
from typing import List, Optional
from urllib.parse import urlparse

import attr
import pandas as pd

from dbnd._core.utils.sql_tracker_common.sql_operation import SqlOperation
from dbnd._core.utils.sql_tracker_common.utils import get_redshift_uri
from dbnd_redshift.sdk.redshift_utils import TEMP_TABLE_NAME, redshift_query
from dbnd_redshift.sdk.wrappers import PostgresConnectionWrapper
from targets.connections import build_conn_path
from targets.providers.pandas.pandas_histograms import PandasHistograms
from targets.value_meta import ColumnStatsArgs, ValueMeta, ValueMetaConf
from targets.values import register_value_type
from targets.values.builtins_values import DataValueType


logger = logging.getLogger(__name__)

NUMERIC_TYPES = [
    "SMALLINT",
    "INT2",
    "INTEGER",
    "INT",
    "INT4",
    "BIGINT",
    "INT8",
    "REAL",
    "FLOAT4",
    "DOUBLE PRECISION",
    "FLOAT8",
    "FLOAT",
    "DECIMAL",
    "NUMERIC",
]


def _strip_quotes_and_lower_case(v: str) -> str:
    without_quotes = v.strip('"') if v else v
    lower_case = without_quotes.lower() if without_quotes else without_quotes
    return lower_case


def _type_conversion(type_name: str) -> str:
    normalized_col_type = "".join(takewhile(lambda c: c != "(", type_name))

    converted_type = type_name

    if normalized_col_type == "character varying":
        converted_type = "str"
    if normalized_col_type == "decimal":
        converted_type = "int"

    return converted_type


def _is_numeric_type(col_type: str):
    return any(num_type in col_type.upper() for num_type in NUMERIC_TYPES)


def get_col_stats_queries(col_name: str, col_type: str, with_percentiles: bool = True):
    most_freq_value_cast = "integer::text" if col_type == "boolean" else "text"

    query_list = [
        f"SELECT '{col_name}' AS name",
        f"(SELECT COUNT(*) FROM {TEMP_TABLE_NAME}) AS row_count",
        f'(SELECT COUNT(*) FROM {TEMP_TABLE_NAME} where "{col_name}" is null) AS null_count',
        f'(SELECT count(distinct "{col_name}") FROM {TEMP_TABLE_NAME}) AS distinct_count',
        f'(SELECT top 1 count("{col_name}") FROM {TEMP_TABLE_NAME} group by "{col_name}" order by count("{col_name}") DESC) AS most_freq_value_count',
        f'(SELECT top 1 "{col_name}" FROM {TEMP_TABLE_NAME} group by "{col_name}" order by count("{col_name}") DESC)::{most_freq_value_cast} AS most_freq_value',
    ]

    if _is_numeric_type(col_type):
        query_list.extend(
            [
                f'(SELECT stddev("{col_name}") FROM {TEMP_TABLE_NAME})::text AS stddev',
                f'(SELECT avg("{col_name}"::float8) FROM {TEMP_TABLE_NAME})::text AS average',  # avg calculates in mem, we're limiting the column datatype so it won't cause numeric overflow
                f'(SELECT min("{col_name}") FROM {TEMP_TABLE_NAME})::text AS minimum',
                f'(SELECT max("{col_name}") FROM {TEMP_TABLE_NAME})::text AS maximum',
            ]
        )
        query_list.extend(_percentile_queries(col_name, with_percentiles))

    else:
        query_list.extend(
            [
                "(SELECT Null) AS stddev",
                "(SELECT Null) AS average",
                "(SELECT Null) AS minimum",
                "(SELECT Null) AS maximum",
            ]
        )
        query_list.extend(_null_queries_for_percentile())

    return query_list


def _percentile_queries(col_name: str, with_percentiles: bool) -> List[str]:
    if not with_percentiles:
        return _null_queries_for_percentile()

    return [
        f'(SELECT percentile_cont(0.25) within group (order by "{col_name}" asc) from {TEMP_TABLE_NAME})::text AS percentile_25',
        f'(SELECT percentile_cont(0.50) within group (order by "{col_name}" asc) from {TEMP_TABLE_NAME})::text AS percentile_50',
        f'(SELECT percentile_cont(0.75) within group (order by "{col_name}" asc) from {TEMP_TABLE_NAME})::text AS percentile_75',
    ]


def _null_queries_for_percentile() -> List[str]:
    return [
        "(SELECT Null) AS percentile_25",
        "(SELECT Null) AS percentile_50",
        "(SELECT Null) AS percentile_75",
    ]


def query_list_to_text(cols_queries_lists: List[List[str]]):
    query = ""
    for col_index, col_query_list in enumerate(cols_queries_lists):
        query += "("
        query += ",\n".join(col_query_list)
        query += ") union all\n" if col_index != len(cols_queries_lists) - 1 else ")\n"

    return query


@attr.s
class RedshiftOperation(SqlOperation):
    database = attr.ib(
        converter=_strip_quotes_and_lower_case, default=None
    )  # type: str
    source_name = attr.ib(
        converter=_strip_quotes_and_lower_case, default=None
    )  # type: str
    host = attr.ib(converter=_strip_quotes_and_lower_case, default=None)  # type: str
    schema_name = attr.ib(
        converter=_strip_quotes_and_lower_case, default=None
    )  # type: str
    table_name = attr.ib(
        converter=_strip_quotes_and_lower_case, default=None
    )  # type: str
    cls_cache = attr.ib(default=None)
    schema_cache = attr.ib(default=None)
    preview_cache: pd.DataFrame = attr.ib(default=None)

    @staticmethod
    def expect_tmp_table(conf):
        return conf.with_stats or conf.with_preview

    def extract_preview(self, connection: PostgresConnectionWrapper):
        """
        Fetches 100 rows from the temporary table for preview
        Args:
            connection: RedShift connection object

        """
        if self.dataframe is not None:
            self.preview_cache = self.dataframe
        elif connection:
            db_preview_query = redshift_query(
                connection.connection, f"SELECT * FROM {TEMP_TABLE_NAME} LIMIT 100"
            )
            if db_preview_query:
                first_row = db_preview_query[0]
                self.preview_cache = pd.DataFrame(
                    db_preview_query, columns=list(first_row.keys())
                )

    def extract_schema(self, connection: PostgresConnectionWrapper):
        """
        Queries redshift for the schema of the table, and formats it in dtypes format
        Args:
            connection: RedShift connection object

        """
        if not connection:
            return

        res_schema = {
            "type": self.__class__.__name__,
            "columns": [],
            "dtypes": {},
            "shape": (),
            "size.bytes": 0,  # TODO: calculate operation size in bytes
        }

        if self.dataframe is not None:
            res_schema.update(
                {
                    "columns": list(self.dataframe.columns),
                    "shape": self.dataframe.shape,
                    "dtypes": {
                        col: str(type_) for col, type_ in self.dataframe.dtypes.items()
                    },
                }
            )
        else:
            if self.table_name is not None:
                redshift_query(
                    connection.connection,
                    f"set search_path to '{self.schema_name}'",
                    fetch_all=False,
                )

                desc_results = redshift_query(
                    connection.connection,
                    f"select * from pg_table_def where tablename='{self.table_name}'",
                )

                if desc_results:
                    for col_desc in desc_results:
                        if len(col_desc) > 2:
                            res_schema["columns"].append(col_desc[2])
                            res_schema["dtypes"][col_desc[2]] = _type_conversion(
                                col_desc[3]
                            )

                res_schema["shape"] = (self.records_count, len(res_schema["columns"]))

                if desc_results is not None:
                    self.schema_cache = res_schema

    def extract_stats(
        self, connection: PostgresConnectionWrapper, with_percentiles: bool
    ):
        """
        Extracts column level stats of data in motion, data is copied to a temp table in redshift (in redshift_tracker)
        and here it is queried to extract column level statistics from it
        The query is constructed with respect to the schema of the table, int values calculates (std, min, max, etc...)
        on top of the normally computed column level stats (nullity, row_count, distinct, frequent values, etc...)

        Example:
            value = RedshiftOperation(...)
            value.compute_stats(redshift_connection)

        """
        stats = []  # type: List[ColumnStatsArgs]

        if not connection:
            logger.warning("No redshift connection, can not extract column level stats")
            return

        if self.schema:
            queries = [
                list(
                    get_col_stats_queries(
                        col, col_type, with_percentiles=with_percentiles
                    )
                )
                for col, col_type in self.schema["dtypes"].items()
            ]

            query = query_list_to_text(queries)

            if not query:
                logger.warning(
                    "redshift_tracker: cannot execute empty query for column level stats"
                )
                return

            column_stats = redshift_query(connection.connection, query)

            if column_stats:
                for col in column_stats:
                    null_percentage = 0
                    if col["row_count"] > 0:
                        null_percentage = (col["null_count"] / col["row_count"]) * 100
                    stats.append(
                        ColumnStatsArgs(
                            column_name=col["name"],
                            column_type=self.schema["dtypes"][col["name"]],
                            records_count=col["row_count"],
                            mean_value=col["average"],
                            min_value=col["minimum"],
                            max_value=col["maximum"],
                            std_value=col["stddev"],
                            quartile_1=col["percentile_25"],
                            quartile_2=col["percentile_50"],
                            quartile_3=col["percentile_75"],
                            distinct_count=col["distinct_count"],
                            null_count=col["null_count"],
                            non_null_count=col["row_count"] - col["null_count"],
                            null_percent=null_percentage,
                            unique_count=col["distinct_count"],
                            most_freq_value=col["most_freq_value"],
                            most_freq_value_count=col["most_freq_value_count"],
                        )
                    )

            self.cls_cache = stats

    def render_connection_path(self, connection) -> str:
        if self.is_file:
            urlparsed = urlparse(self.source_name)
            return build_conn_path(
                conn_type=urlparsed.scheme,
                hostname=urlparsed.netloc,
                port=urlparsed.port,
                path=urlparsed.path,
            )
        else:
            return get_redshift_uri(
                self.host, self.database, self.schema_name, self.table_name
            )

    @property
    def columns(self) -> Optional[List[str]]:
        try:
            return list(self.schema["columns"])
        except TypeError:
            return None

    @property
    def columns_count(self) -> Optional[int]:
        try:
            return len(self.schema["dtypes"])
        except TypeError:
            return None

    @property
    def preview(self):
        if self.preview_cache is not None:
            return self.preview_cache.to_string(index=False, max_rows=10)
        return None

    @property
    def schema(self):
        return self.schema_cache

    @property
    def column_stats(self):
        return self.cls_cache


class RedshiftTableValueType(DataValueType):
    type = RedshiftOperation

    support_merge = False
    is_lazy_evaluated = True

    def get_value_meta(self, value: RedshiftOperation, meta_conf: ValueMetaConf):

        # currently, histograms are not supported
        histograms = {}
        hist_sys_metrics = None

        dimensions = (value.records_count, None)

        data_schema = None
        if meta_conf.log_schema:
            data_schema = value.schema
            dimensions = value.schema["shape"]

        column_stats = {}
        if meta_conf.log_stats:
            if value.dataframe is not None:
                column_stats, _ = PandasHistograms(
                    value.dataframe, meta_conf
                ).get_histograms_and_stats()
            else:
                column_stats = value.column_stats

        preview = ""
        if meta_conf.log_preview:
            preview = value.preview

        return ValueMeta(
            value_preview=preview,
            data_dimensions=dimensions,
            data_schema=data_schema,
            data_hash=str(hash(self.to_signature(value))),
            columns_stats=column_stats,
            histogram_system_metrics=hist_sys_metrics,
            histograms=histograms,
            query=value.query,
        )


register_value_type(RedshiftTableValueType())
