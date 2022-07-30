# © Copyright Databand.ai, an IBM Company 2022

import pytest

from mock.mock import MagicMock

from dbnd_redshift.sdk.redshift_connection_extractor import get_redshift_dataset


@pytest.fixture
def redshift_connection():
    mock_connection = MagicMock()
    mock_connection.schema = "conn_schema"
    mock_connection.port = 1234
    mock_connection.info.options = ""
    mock_connection.info.dsn_parameters = {
        "host": "redshift.mock.test",
        "dbname": "mock_db",
    }
    return mock_connection


@pytest.mark.parametrize(
    "query, conn_options, database, schema, table",
    [
        [
            """
            COPY test_db
            FROM 's3://dbnd-redshift/leap_year.csv'
            iam_role 'arn:aws:iam::410311604149:role/myRedshiftRole' CSV
            IGNOREHEADER 1
            """,
            "-c search_path='search_path_schema'",
            "mock_db",
            "public",
            "test_db",
        ],
        [
            """
            COPY test_db (column_a, column_b)
            FROM 's3://dbnd-redshift/leap_year.csv'
            iam_role 'arn:aws:iam::410311604149:role/myRedshiftRole' CSV
            """,
            "",
            "mock_db",
            "public",
            "test_db",
        ],
        [
            """
            COPY test_schema.test_db (column_a, column_b)
            FROM 's3://dbnd-redshift/leap_year.csv'
            iam_role 'arn:aws:iam::410311604149:role/myRedshiftRole' CSV
            """,
            "-c search_path='mock_options_schema'",
            "mock_db",
            "test_schema",
            "test_db",
        ],
        [
            """
            COPY test_schema.test_db
            FROM 's3://dbnd-redshift/leap_year.csv'
            iam_role 'arn:aws:iam::410311604149:role/myRedshiftRole' CSV
            """,
            "",
            "mock_db",
            "test_schema",
            "test_db",
        ],
    ],
)
def test_redshift_dataset_uris(
    query, conn_options, database, schema, table, redshift_connection
):
    redshift_connection.info.options = conn_options
    assert get_redshift_dataset(redshift_connection, query).database == database
    assert get_redshift_dataset(redshift_connection, query).schema == schema
    assert get_redshift_dataset(redshift_connection, query).table == table
