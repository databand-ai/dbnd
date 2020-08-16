from typing import Dict, Optional, Tuple

import attr
import psycopg2
import yaml

from psycopg2.extras import RealDictCursor

from dbnd._core.tracking.histograms import HistogramDataType, HistogramSpec
from dbnd._vendor.tabulate import tabulate
from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values import register_value_type
from targets.values.builtins_values import DataValueType


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

    def get_all_data_columns(self, value):
        # type: (PostgresTable) -> Dict[str, HistogramDataType]
        types_map = {
            "boolean": HistogramDataType.boolean,
            "integer": HistogramDataType.numeric,
            "real": HistogramDataType.numeric,
            "smallint": HistogramDataType.numeric,
        }
        with PostgresController(value.connection_string, value.table_name) as postgres:
            pg_types = postgres.get_column_types()
        return {
            column: types_map.get(pg_type, HistogramDataType.string)
            for column, pg_type in pg_types.items()
        }

    def get_value_meta(self, value, meta_conf):
        # type: (PostgresTable, ValueMetaConf) -> ValueMeta
        stats, histograms, data_schema, data_preview, column_name_to_type = [None] * 5

        with PostgresController(value.connection_string, value.table_name) as postgres:
            if meta_conf.log_histograms:
                histogram_spec = meta_conf.get_histogram_spec(
                    self, PostgresTable(postgres.table_name, postgres.connection_string)
                )
                stats, histograms = postgres.get_histograms_and_stats(histogram_spec)
            if meta_conf.log_preview:
                data_preview = postgres.to_preview()
            if meta_conf.log_schema:
                data_schema = {
                    "type": self.type_str,
                    "column_types": postgres.get_column_types(),
                }

        return ValueMeta(
            value_preview=data_preview,
            data_dimensions=None,
            data_schema=data_schema,
            data_hash=self.to_signature(value),
            descriptive_stats=stats,
            histograms=histograms,
        )


register_value_type(PostgresTableValueType())


class PostgresController:
    """ Interacts with postgres, queries it, and calculates histograms and stats """

    def __init__(self, connection_string, table_name):
        self.table_name = table_name
        self.connection_string = connection_string
        self._connection = None
        self._column_types = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection is not None:
            self._connection.close()

    def to_preview(self):
        rows = self._query("select * from {} limit 20".format(self.table_name))
        preview_table = tabulate(rows, headers="keys")
        return preview_table

    def get_column_types(self):
        # type: () -> Dict[str, str]
        if self._column_types is not None:
            return self._column_types

        results = self._query(
            "select column_name, data_type from information_schema.columns where table_name = %s",
            self.table_name,
        )
        self._column_types = {row["column_name"]: row["data_type"] for row in results}
        return self._column_types

    def get_histograms_and_stats(self, histogram_spec):
        # type: (HistogramSpec) -> Tuple[Dict, Dict]

        if histogram_spec.none:
            return {}, {}

        pg_stats = self._query(
            "select * from pg_stats where tablename = %s", self.table_name
        )
        count = self._get_row_count()

        histograms, stats = dict(), dict()
        column_name_to_type = self.get_column_types()
        for pg_stat_row in pg_stats:
            column_name = pg_stat_row["attname"]
            if column_name not in histogram_spec.columns:
                continue
            column_type = column_name_to_type[column_name]
            column_stats, column_histogram = self._get_column_histogram_and_stats(
                pg_stat_row, count, column_type, histogram_spec
            )
            if column_histogram is not None:
                histograms[column_name] = column_histogram
            stats[column_name] = column_stats

        return stats, histograms

    def _get_column_histogram_and_stats(
        self, pg_stats_row, count, column_type, histogram_spec
    ):
        # type: (..., ..., ..., HistogramSpec) -> Tuple[Dict, Optional[Tuple]]
        stats = self._calculate_stats(count, pg_stats_row)
        stats["type"] = column_type
        if histogram_spec.only_stats:
            return stats, None
        common_counts, common_values = self._get_common_values(count, pg_stats_row)

        # types according to postgres documentation:
        # https://www.postgresql.org/docs/9.5/datatype-numeric.html
        # https://www.postgresql.org/docs/9.5/datatype-character.html
        if column_type in (
            "character varying",
            "varchar",
            "character",
            "char",
            "text",
            "boolean",
        ):
            if (common_values is None) or (common_counts is None):
                histogram = None
            else:
                histogram = (common_counts, common_values)
                self._add_others_to_histogram(histogram, stats)
        elif column_type in (
            "smallint",
            "integer",
            "bigint",
            "decimal",
            "numeric",
            "real",
            "double",
            "smallserial",
            "serial",
            "bigserial",
        ):
            histogram = self._calculate_numerical_histogram(
                pg_stats_row, count, stats["null-count"]
            )
            histogram = self._add_common_values_to_histogram(
                histogram, common_counts, common_values
            )
        else:
            histogram = None

        return stats, histogram

    def _get_row_count(self):
        result = self._query(
            "select * from pg_class where relname = %s", self.table_name
        )
        return int(result[0]["reltuples"])

    def _calculate_stats(self, count, pg_stats_row):
        # type: (...) -> Dict
        stats = dict()
        stats["null-count"] = int(pg_stats_row["null_frac"] * count)
        stats["count"] = count
        distinct = pg_stats_row["n_distinct"]
        stats["distinct"] = distinct if (distinct > 0) else (distinct * -1 * count)
        stats["distinct"] = int(stats["distinct"])
        return stats

    def _get_common_values(self, count, pg_stats_row):
        common_values = pg_stats_row["most_common_vals"]
        common_frequencies = pg_stats_row["most_common_freqs"]

        if (common_frequencies is None) or (common_values is None):
            return None, None

        common_counts = [int(freq * count) for freq in common_frequencies]
        common_values = self._pg_anyarray_to_list(common_values)
        return common_counts, common_values

    def _calculate_numerical_histogram(self, pg_stats_row, count, null_count):
        values_str = pg_stats_row["histogram_bounds"]
        values = self._pg_anyarray_to_list(values_str)

        buckets = len(values) - 1
        bucket_count = (count - null_count) / buckets
        bucket_count = int(bucket_count)
        counts = [bucket_count] * buckets

        return counts, values

    def _add_common_values_to_histogram(self, histogram, common_counts, common_values):
        if (common_counts is None) or (common_values is None):
            return histogram

        histogram_counts, histogram_values = histogram

        for value, count in zip(common_values, common_counts):
            for i, histogram_value in enumerate(histogram_values):
                if value < histogram_value:
                    histogram_counts[i - 1] += count

        return histogram

    def _add_others_to_histogram(self, histogram, stats):
        """ Add a bucket for all least common values, called '_others' """
        counts, values = histogram
        if not values or stats["distinct"] <= len(values):
            return histogram

        others_count = stats["count"] - stats["null-count"] - sum(counts)
        counts.append(others_count)
        values.append("_others")
        return histogram

    def _pg_anyarray_to_list(self, value):
        """ postgres returns anyarray type as a string, this function converts it to a list """
        value = value.strip("{}")
        value = "[" + value + "]"
        return yaml.safe_load(value)

    def _query(self, query, *args):
        if self._connection is None:
            self._connection = psycopg2.connect(
                self.connection_string, cursor_factory=RealDictCursor
            )

        cursor = self._connection.cursor()
        cursor.execute(query, args)
        result = cursor.fetchall()
        return result
