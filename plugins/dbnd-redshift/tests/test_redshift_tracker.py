import pandas as pd
import psycopg2
import pytest

from mock import Mock, patch

from dbnd._core.constants import DbndTargetOperationType
from dbnd._core.sql_tracker_common.sql_extract import Column
from dbnd._core.sql_tracker_common.sql_operation import SqlOperation
from dbnd_redshift.sdk.redshift_tracker import COPY_ROWS_COUNT_QUERY, RedshiftTracker


NUMBER_OF_ROWS_INSERTED = 100

ERROR_MESSAGE = "Exception Mock"

COPY_INTO_TABLE_FROM_S3_FILE_QUERY = """COPY "MY_TABLE" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv"""

COPY_INTO_TABLE_FAIL_QUERY = """copy into FAIL from s3://test/test.json CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');"""

COPY_INTO_TABLE_FROM_S3_FILE_GRACEFUL_FAIL_QUERY = """COPY "schema_fail" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv"""

GET_SCHEMA_FAIL_QUERY = "select * from pg_table_def where tablename='schema_fail'"


class CursorMock:
    def __init__(self, connection_mock):
        self.connection_mock = connection_mock
        self.query = None

    def execute(self, query, vars=None):
        if query == COPY_INTO_TABLE_FAIL_QUERY:
            raise Exception(ERROR_MESSAGE)
        self.query = query

    def fetchall(self):
        if self.query == COPY_ROWS_COUNT_QUERY or self.query == GET_SCHEMA_FAIL_QUERY:
            return [[NUMBER_OF_ROWS_INSERTED]]
        elif self.query == "select * from pg_table_def where tablename='my_table'":
            return [
                ("public", "my_table", "column_1", "integer"),
                ("public", "my_table", "column_2", "character varying(256)"),
            ]

    @property
    def connection(self):
        return self.connection_mock

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class ConnectionInfoMock:
    @property
    def host(self):
        return "test"

    @property
    def port(self):
        return 12345

    @property
    def dbname(self):
        return "db"


class ConnectionMock:
    def __init__(self):
        self.mock_cursor = CursorMock(self)
        self.info = ConnectionInfoMock()

    def cursor(self):
        return self.mock_cursor

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture
def mock_redshift():
    execute_mock = Mock()

    def _connect(*args, **kwargs):
        return ConnectionMock()

    with patch.object(
        psycopg2, "connect", new=_connect,
    ):
        yield execute_mock


def _redshift_connect():
    return psycopg2.connect(
        host="test", port=12345, database="db", user="user", password="password",
    )


def test_copy_into_s3(mock_redshift):
    expected = [
        SqlOperation(
            extracted_schema={
                "s3://my/bucket/file.csv.*": [
                    Column(
                        dataset_name="s3://my/bucket/file.csv",
                        name="*",
                        alias="s3://my/bucket/file.csv.*",
                        is_file=True,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=NUMBER_OF_ROWS_INSERTED,
            query="COPY \"MY_TABLE\" from s3://my/bucket/file.csv iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
        ),
        SqlOperation(
            extracted_schema={
                '"MY_TABLE".*': [
                    Column(
                        dataset_name='"MY_TABLE"',
                        name="*",
                        alias='"MY_TABLE".*',
                        is_file=False,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=NUMBER_OF_ROWS_INSERTED,
            query="COPY \"MY_TABLE\" from s3://my/bucket/file.csv iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
        ),
    ]
    return run_tracker_custom_query(COPY_INTO_TABLE_FROM_S3_FILE_QUERY, expected)


def test_copy_into_s3_set_read_schema(mock_redshift):
    expected = [
        SqlOperation(
            extracted_schema={
                "s3://my/bucket/file.csv.*": [
                    Column(
                        dataset_name="s3://my/bucket/file.csv",
                        name="*",
                        alias="s3://my/bucket/file.csv.*",
                        is_file=True,
                        is_stage=False,
                    )
                ]
            },
            dtypes=dict(c1="object", c2="int64"),
            records_count=NUMBER_OF_ROWS_INSERTED,
            query="COPY \"MY_TABLE\" from s3://my/bucket/file.csv iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
        ),
        SqlOperation(
            extracted_schema={
                '"MY_TABLE".*': [
                    Column(
                        dataset_name='"MY_TABLE"',
                        name="*",
                        alias='"MY_TABLE".*',
                        is_file=False,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=NUMBER_OF_ROWS_INSERTED,
            query="COPY \"MY_TABLE\" from s3://my/bucket/file.csv iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
        ),
    ]
    return run_tracker_custom_query(
        COPY_INTO_TABLE_FROM_S3_FILE_QUERY,
        expected,
        pd.DataFrame(data=[["p", 1]], columns=["c1", "c2"]),
    )


def test_copy_into_s3_set_read_schema_wrong_df(mock_redshift):
    expected = [
        SqlOperation(
            extracted_schema={
                "s3://my/bucket/file.csv.*": [
                    Column(
                        dataset_name="s3://my/bucket/file.csv",
                        name="*",
                        alias="s3://my/bucket/file.csv.*",
                        is_file=True,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=NUMBER_OF_ROWS_INSERTED,
            query="COPY \"MY_TABLE\" from s3://my/bucket/file.csv iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
        ),
        SqlOperation(
            extracted_schema={
                '"MY_TABLE".*': [
                    Column(
                        dataset_name='"MY_TABLE"',
                        name="*",
                        alias='"MY_TABLE".*',
                        is_file=False,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=NUMBER_OF_ROWS_INSERTED,
            query="COPY \"MY_TABLE\" from s3://my/bucket/file.csv iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
        ),
    ]
    return run_tracker_custom_query(
        COPY_INTO_TABLE_FROM_S3_FILE_QUERY, expected, ["c1", "c2"],
    )


def test_copy_into_operation_failure(mock_redshift):
    expected = [
        SqlOperation(
            extracted_schema={
                "s3://test/test.json.*": [
                    Column(
                        dataset_name="s3://test/test.json",
                        name="*",
                        alias="s3://test/test.json.*",
                        is_file=True,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=NUMBER_OF_ROWS_INSERTED,
            query="copy into FAIL from s3://test/test.json CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');",
            query_id=None,
            success=False,
            op_type=DbndTargetOperationType.read,
            error="Exception Mock",
        ),
        SqlOperation(
            extracted_schema={
                "FAIL.*": [
                    Column(
                        dataset_name="FAIL",
                        name="*",
                        alias="FAIL.*",
                        is_file=False,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=NUMBER_OF_ROWS_INSERTED,
            query="copy into FAIL from s3://test/test.json CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');",
            query_id=None,
            success=False,
            op_type=DbndTargetOperationType.write,
            error="Exception Mock",
        ),
    ]
    with pytest.raises(Exception) as e:
        run_tracker_custom_query(COPY_INTO_TABLE_FAIL_QUERY, expected)
    assert str(e.value) == ERROR_MESSAGE


def test_copy_into_s3_graceful_failure(mock_redshift):
    expected = [
        SqlOperation(
            extracted_schema={
                "s3://my/bucket/file.csv.*": [
                    Column(
                        dataset_name="s3://my/bucket/file.csv",
                        name="*",
                        alias="s3://my/bucket/file.csv.*",
                        is_file=True,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=NUMBER_OF_ROWS_INSERTED,
            query="COPY \"schema_fail\" from s3://my/bucket/file.csv iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
        ),
        SqlOperation(
            extracted_schema={
                '"schema_fail".*': [
                    Column(
                        dataset_name='"schema_fail"',
                        name="*",
                        alias='"schema_fail".*',
                        is_file=False,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=NUMBER_OF_ROWS_INSERTED,
            query="COPY \"schema_fail\" from s3://my/bucket/file.csv iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
        ),
    ]
    return run_tracker_custom_query(
        COPY_INTO_TABLE_FROM_S3_FILE_GRACEFUL_FAIL_QUERY, expected
    )


def run_tracker_custom_query(query, expected, dataframe=None):
    redshift_tracker = RedshiftTracker()
    with redshift_tracker as tracker:
        with _redshift_connect() as con:
            c = con.cursor()
            try:
                c.execute(query)
                if dataframe is not None:
                    tracker.set_schema(dataframe)
            finally:
                assert redshift_tracker.operations == expected
        # flush operations
    assert redshift_tracker.operations == []
