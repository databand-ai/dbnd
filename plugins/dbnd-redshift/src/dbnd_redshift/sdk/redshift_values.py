import logging

from itertools import takewhile
from typing import List
from urllib.parse import urlparse

import attr
import pandas as pd

from dbnd._core.sql_tracker_common.sql_operation import SqlOperation
from dbnd_redshift.sdk.redshift_utils import TEMP_TABLE_NAME, redshift_query
from dbnd_redshift.sdk.wrappers import PostgresConnectionWrapper
from targets.connections import build_conn_path
from targets.value_meta import ColumnStatsArgs, ValueMeta, ValueMetaConf
from targets.values import register_value_type
from targets.values.builtins_values import DataValueType
from targets.values.pandas_histograms import PandasHistograms


logger = logging.getLogger(__name__)


def _strip_quotes(v: str) -> str:
    return v.strip('"') if v else v


def _type_conversion(type_name: str) -> str:
    normalized_col_type = "".join(takewhile(lambda c: c != "(", type_name))

    converted_type = type_name

    if normalized_col_type == "character varying":
        converted_type = "str"
    if normalized_col_type == "decimal":
        converted_type = "int"

    return converted_type


@attr.s
class RedshiftOperation(SqlOperation):
    database = attr.ib(converter=_strip_quotes, default=None)  # type: str
    target_name = attr.ib(converter=_strip_quotes, default=None)  # type: str
    source_name = attr.ib(converter=_strip_quotes, default=None)  # type: str
    cls_cache = attr.ib(default=None)
    schema_cache = attr.ib(default=None)
    preview_cache = attr.ib(default=None)

    def extract_preview(self, connection: PostgresConnectionWrapper):
        """
        Fetches 100 rows from the temporary table for preview
        Args:
            connection: RedShift connection object

        """
        if connection:
            self.preview_cache = redshift_query(
                connection.connection, f"SELECT * FROM {TEMP_TABLE_NAME} LIMIT 100"
            )

    def extract_schema(self, connection: PostgresConnectionWrapper):
        """
        Queries redshift for the schema of the table, and formats it in dtypes format
        Args:
            connection: RedShift connection object

        """
        res_schema = {
            "type": self.__class__.__name__,
            "columns": [],
            "dtypes": {},
            "shape": (),
            "size.bytes": 0,  # TODO: calculate operation size in bytes
        }

        if connection:
            if self.dataframe is not None:
                res_schema.update(
                    {
                        "columns": list(self.dataframe.columns),
                        "shape": self.dataframe.shape,
                        "dtypes": {
                            col: str(type_)
                            for col, type_ in self.dataframe.dtypes.items()
                        },
                    }
                )
            else:
                desc_results = redshift_query(
                    connection.connection,
                    f"select * from pg_table_def where tablename='{self.target_name.lower()}'",
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

                self.preview_cache = redshift_query(
                    connection.connection, f"SELECT * FROM {TEMP_TABLE_NAME} LIMIT 100"
                )

    def extract_stats(self, connection: PostgresConnectionWrapper):
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

        if connection:
            query = ""

            if self.schema:
                for index, key in enumerate(self.schema["dtypes"]):
                    query += f"""(SELECT
                                    '{key}' as name,
                                    (SELECT COUNT("{key}") FROM {TEMP_TABLE_NAME}) AS row_count,
                                    (SELECT COUNT("{key}") FROM {TEMP_TABLE_NAME} where "{key}" is null) AS null_count,
                                    (SELECT count(distinct "{key}") FROM {TEMP_TABLE_NAME}) AS distinct_count,
                                    (SELECT top 1 "{key}" FROM {TEMP_TABLE_NAME} group by "{key}" order by count("{key}") DESC)::varchar AS most_freq_value,
                                    (SELECT top 1 count("{key}") FROM {TEMP_TABLE_NAME} group by "{key}" order by count("{key}") DESC) AS most_freq_value_count
                                """
                    if self.schema["dtypes"][key] == "integer":
                        query += f""",
                                    (SELECT stddev("{key}") FROM {TEMP_TABLE_NAME}) AS stddev,
                                    (SELECT avg("{key}") FROM {TEMP_TABLE_NAME}) AS average,
                                    (SELECT min("{key}") from {TEMP_TABLE_NAME}) AS minimum,
                                    (SELECT max("{key}") FROM {TEMP_TABLE_NAME}) AS maximum,
                                    (SELECT percentile_cont(0.25) within group (order by "{key}" asc) from {TEMP_TABLE_NAME}) AS percentile_25,
                                    (SELECT percentile_cont(0.50) within group (order by "{key}" asc) from {TEMP_TABLE_NAME}) AS percentile_50,
                                    (SELECT percentile_cont(0.75) within group (order by "{key}" asc) from {TEMP_TABLE_NAME}) AS percentile_75
                                """
                    else:
                        query += f""",
                                    (Null) AS stddev,
                                    (Null) AS average,
                                    (Null) AS minimum,
                                    (Null) AS maximum,
                                    (Null) AS percentile_25,
                                    (Null) AS percentile_50,
                                    (Null) AS percentile_75
                                """

                    if index != len(self.schema["dtypes"]) - 1:
                        query += ") union all"
                    else:
                        query += ")"

                column_stats = redshift_query(connection.connection, query)

                if column_stats:
                    for col in column_stats:
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
                                null_percent=(col["null_count"] / col["row_count"])
                                * 100,
                                unique_count=col["distinct_count"],
                                most_freq_value=col["most_freq_value"],
                                most_freq_value_count=col["most_freq_value_count"],
                            )
                        )

                self.cls_cache = stats
        else:
            logger.warning("No redshift connection, can not extract column level stats")

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
            return build_conn_path(
                conn_type="redshift",
                hostname=connection.host,
                port=connection.port,
                path=self.target_name,
            )

    @property
    def columns(self) -> List[str]:
        try:
            return list(self.schema["columns"])
        except TypeError:
            return None

    @property
    def columns_count(self) -> int:
        try:
            return len(self.schema["dtypes"])
        except TypeError:
            return None

    @property
    def preview(self):
        if self.dataframe is not None:
            return self.dataframe.to_string(index=False, max_rows=10)
        else:
            if self.preview_cache:
                df = pd.DataFrame(self.preview_cache)
                return df.to_string(index=False, max_rows=10)
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

        dimensions = None
        if meta_conf.log_size:
            dimensions = value.schema["shape"]

        data_schema = None
        if meta_conf.log_schema:
            data_schema = value.schema

        column_stats = None
        if meta_conf.log_stats:
            if value.dataframe is not None:
                column_stats, _ = PandasHistograms(
                    value.dataframe, meta_conf
                ).get_histograms_and_stats()
            else:
                column_stats = value.column_stats

        return ValueMeta(
            value_preview=value.preview,
            data_dimensions=dimensions,
            data_schema=data_schema,
            data_hash=str(hash(self.to_signature(value))),
            columns_stats=column_stats,
            histogram_system_metrics=hist_sys_metrics,
            histograms=histograms,
        )


register_value_type(RedshiftTableValueType())
