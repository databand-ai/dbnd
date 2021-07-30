import typing

import psycopg2
import yaml

from psycopg2.extras import RealDictCursor

from dbnd._vendor.tabulate import tabulate


if typing.TYPE_CHECKING:
    from typing import Dict, Tuple, Optional, List
    from targets.value_meta import ValueMetaConf
    from dbnd._core.tracking.log_data_request import LogDataRequest


class PostgresController(object):
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

    def get_histograms_and_stats(self, meta_conf):
        # type: (ValueMetaConf) -> Tuple[Dict[str, Dict], Dict[str, Tuple]]
        pg_stats = self._query(
            "select * from pg_stats where tablename = %s", self.table_name
        )
        count = self._get_row_count()

        histograms, stats = dict(), dict()
        column_name_to_type = self.get_column_types()
        columns_to_calc = self._get_columns_from_request(meta_conf.log_histograms)

        for pg_stat_row in pg_stats:
            column_name = pg_stat_row["attname"]
            column_type = column_name_to_type[column_name]
            column_stats, column_histogram = self._get_column_histogram_and_stats(
                pg_stat_row, count, column_type
            )
            if (column_histogram is not None) and (column_name in columns_to_calc):
                histograms[column_name] = column_histogram

            # we always get stats since it doesn't have any performance cost
            stats[column_name] = column_stats

        return stats, histograms

    def _get_columns_from_request(self, data_request):
        # type: (LogDataRequest) -> List[str]
        columns_to_calc = list(data_request.include_columns)
        column_types = self.get_column_types()
        for column_name, column_type in column_types.items():
            if data_request.include_all_string and self._is_string_column(column_type):
                columns_to_calc.append(column_name)
            elif data_request.include_all_boolean and column_type == "boolean":
                columns_to_calc.append(column_name)
            elif data_request.include_all_numeric and self._is_numeric_column(
                column_type
            ):
                columns_to_calc.append(column_name)

        columns_to_calc = [
            column
            for column in columns_to_calc
            if column not in data_request.exclude_columns
        ]
        return columns_to_calc

    def _is_numeric_column(self, column_type):
        return column_type in (
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
        )

    def _is_string_column(self, column_type):
        return column_type in (
            "character varying",
            "varchar",
            "character",
            "char",
            "text",
            "boolean",
        )

    def _is_categorical_column(self, column_type):
        return self._is_string_column(column_type) or column_type == "boolean"

    def _get_column_histogram_and_stats(self, pg_stats_row, count, column_type):
        # type: (Dict, int, str) -> Tuple[Dict, Optional[Tuple]]
        stats = self._calculate_stats(count, pg_stats_row)
        stats["type"] = column_type
        common_counts, common_values = self._get_common_values(count, pg_stats_row)

        # types according to postgres documentation:
        # https://www.postgresql.org/docs/9.5/datatype-numeric.html
        # https://www.postgresql.org/docs/9.5/datatype-character.html
        if self._is_categorical_column(column_type):
            if (common_values is None) or (common_counts is None):
                histogram = None
            else:
                histogram = (common_counts, common_values)
                self._add_others_to_histogram(histogram, stats)
        elif self._is_numeric_column(column_type):
            histogram = self._calculate_numeric_histogram(
                pg_stats_row, count, stats["null-count"]
            )
            histogram = self._add_common_values_to_histogram(
                histogram, common_counts, common_values
            )
        else:
            histogram = None

        return stats, histogram

    def _get_row_count(self):
        # type: () -> int
        result = self._query(
            "select * from pg_class where relname = %s", self.table_name
        )
        return int(result[0]["reltuples"])

    def _calculate_stats(self, count, pg_stats_row):
        # type: (int, Dict) -> Dict
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

    def _calculate_numeric_histogram(self, pg_stats_row, count, null_count):
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
        # type: (str) -> List
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
