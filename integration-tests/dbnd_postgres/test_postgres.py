import os

import psycopg2

from psycopg2.extras import execute_values
from pytest import fixture

from dbnd_postgres import postgres_values
from targets.value_meta import ValueMetaConf


class TestPostgres(object):
    connection_string = os.getenv("DBND__WEBSERVER__SQL_ALCHEMY_CONN")

    @fixture(scope="module")
    def table(self):
        with psycopg2.connect(self.connection_string) as connection:
            table_name = "test_table"
            cursor = connection.cursor()
            query = "CREATE TABLE {} (string_value varchar(20), numerical_value integer, boolean_value boolean)".format(
                table_name
            )
            cursor.execute(query)

            query = "INSERT INTO {} VALUES %s".format(table_name)
            strings = (
                ["Hello World!"] * 100
                + ["Shalom Olam!"] * 500
                + ["Ola Mundo!"] * 300
                + [None] * 100
            )
            numbers = [None] * 50 + list(range(900)) + [None] * 50
            booleans = [True] * 200 + [False] * 700 + [None] * 100
            values = zip(strings, numbers, booleans)
            execute_values(cursor, query, values)

            cursor.execute("analyze " + table_name)
            connection.commit()
            yield table_name

            query = "DROP TABLE test_table"
            cursor.execute(query)

    def test_dbnd_postgres(self, table):
        meta_conf = ValueMetaConf(log_schema=True, log_histograms=True)
        value = postgres_values.PostgresTable(table, self.connection_string)
        value_meta = postgres_values.PostgresTableValueType().get_value_meta(
            value, meta_conf=meta_conf
        )

        expected_stats = {
            "string_value": {
                "null-count": 100,
                "count": 1000,
                "distinct": 3,
                "type": "character varying",
            },
            "numerical_value": {
                "null-count": 100,
                "count": 1000,
                "distinct": 900,
                "type": "integer",
            },
            "boolean_value": {
                "null-count": 100,
                "count": 1000,
                "distinct": 2,
                "type": "boolean",
            },
        }
        assert value_meta.descriptive_stats == expected_stats

        expected_histograms = {
            "string_value": (
                [500, 300, 100],
                ["Shalom Olam!", "Ola Mundo!", "Hello World!"],
            ),
            "numerical_value": (
                [90, 90, 90, 90, 90, 90, 90, 90, 90, 90],
                [0, 89, 179, 269, 359, 449, 539, 629, 719, 809, 899],
            ),
            "boolean_value": ([700, 200], ["f", "t"]),
        }
        # TODO: Fix numerical value inconsistency
        assert (
            value_meta.histograms["string_value"] == expected_histograms["string_value"]
        )
        assert (
            value_meta.histograms["boolean_value"]
            == expected_histograms["boolean_value"]
        )

        expected_schema = {
            "type": "PostgresTable",
            "column_types": {
                "string_value": "character varying",
                "numerical_value": "integer",
                "boolean_value": "boolean",
            },
        }
        assert value_meta.data_schema == expected_schema
