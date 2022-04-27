import pandas as pd
import psycopg2
import pytest

from mock import Mock, patch

from dbnd._core.constants import DbndTargetOperationType
from dbnd._core.sql_tracker_common.sql_extract import Column
from dbnd_redshift.sdk.redshift_tracker import RedshiftTracker
from dbnd_redshift.sdk.redshift_utils import COPY_ROWS_COUNT_QUERY
from dbnd_redshift.sdk.redshift_values import (
    RedshiftOperation,
    get_col_query,
    query_list_to_text,
)


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

    def cursor(self, cursor_factory=None):
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

    with patch.object(psycopg2, "connect", new=_connect):
        yield execute_mock


def _redshift_connect():
    return psycopg2.connect(
        host="test", port=12345, database="db", user="user", password="password"
    )


def compare_eq_op(operation: RedshiftOperation, expected: RedshiftOperation):
    tests = {
        "schema": operation.extracted_schema == expected.extracted_schema,
        "query": operation.query == expected.query,
        "source_name": operation.source_name == expected.source_name,
        "target_name": operation.target_name == expected.target_name,
        "op_type": operation.op_type == expected.op_type,
    }

    for test in tests.keys():
        if not tests[test]:
            return False

    return True


def compare_eq_operations(
    operations: [RedshiftOperation], expected: [RedshiftOperation]
):
    for op in operations:
        match = False
        for expectation in expected:
            if compare_eq_op(op, expectation):
                match = True
                break

        if not match:
            return False

    return True


schema = {
    "unique_key": "int",
    "created_date": "str",
    "agency": "str",
    "agency_name": "boolean",
}


def test_boolean_type_get_col_query():
    query = get_col_query("column1", "boolean")
    for q in query:
        if q.endswith("AS most_freq_value"):
            assert "integer::text" in q


def test_extract_stats_query():
    queries = [list(get_col_query(col, col_type)) for col, col_type in schema.items()]
    query = query_list_to_text(queries)
    assert all(k in query for k in schema)


def test_multiple_psycopg2_connections(mock_redshift):
    expected1 = [
        RedshiftOperation(
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
            query="COPY \"MY_TABLE\" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
            source_name="s3://my/bucket/file.csv",
            target_name="MY_TABLE",
        ),
        RedshiftOperation(
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
            query="COPY \"MY_TABLE\" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
            source_name="s3://my/bucket/file.csv",
            target_name="MY_TABLE",
        ),
    ]
    expected2 = [
        RedshiftOperation(
            extracted_schema={
                "s3://my/bucket/file2.csv.*": [
                    Column(
                        dataset_name="s3://my/bucket/file2.csv",
                        name="*",
                        alias="s3://my/bucket/file2.csv.*",
                        is_file=True,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=NUMBER_OF_ROWS_INSERTED,
            query="COPY \"MY_TABLE\" from 's3://my/bucket/file2.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
            source_name="s3://my/bucket/file2.csv",
            target_name="MY_TABLE",
        ),
        RedshiftOperation(
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
            query="COPY \"MY_TABLE\" from 's3://my/bucket/file2.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
            source_name="s3://my/bucket/file2.csv",
            target_name="MY_TABLE",
        ),
    ]

    COPY_INTO_TABLE_FROM_S3_FILE_QUERY_2 = """COPY "MY_TABLE" from 's3://my/bucket/file2.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv"""

    redshift_tracker = RedshiftTracker()

    with redshift_tracker as tracker:
        with _redshift_connect() as con1, _redshift_connect() as con2:
            c1 = con1.cursor()
            c2 = con2.cursor()

            try:
                c1.execute(COPY_INTO_TABLE_FROM_S3_FILE_QUERY)
                c2.execute(COPY_INTO_TABLE_FROM_S3_FILE_QUERY_2)
            finally:
                assert compare_eq_operations(
                    redshift_tracker.connections.get_operations(c1), expected1
                )
                assert compare_eq_operations(
                    redshift_tracker.connections.get_operations(c2), expected2
                )
        # flush operations
    assert redshift_tracker.connections.get_operations(c1) == []
    assert redshift_tracker.connections.get_operations(c2) == []


def test_copy_into_s3(mock_redshift):
    expected = [
        RedshiftOperation(
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
            query="COPY \"MY_TABLE\" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
            source_name="s3://my/bucket/file.csv",
            target_name="MY_TABLE",
        ),
        RedshiftOperation(
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
            query="COPY \"MY_TABLE\" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
            source_name="s3://my/bucket/file.csv",
            target_name="MY_TABLE",
        ),
    ]
    return run_tracker_custom_query(COPY_INTO_TABLE_FROM_S3_FILE_QUERY, expected)


def test_copy_into_s3_set_read_schema(mock_redshift):
    operations = run_tracker_custom_df(
        COPY_INTO_TABLE_FROM_S3_FILE_QUERY,
        dataframe=pd.DataFrame(data=[["p", 1]], columns=["c1", "c2"]),
    )

    for op in operations:
        assert op.columns == ["c1", "c2"]
        assert op.columns_count == 2
        assert op.dtypes == {"c1": "object", "c2": "int64"}


def test_copy_into_s3_set_read_schema_wrong_df(mock_redshift):
    operations = run_tracker_custom_df(COPY_INTO_TABLE_FROM_S3_FILE_QUERY, ["c1", "c2"])

    for op in operations:
        if op.op_type == DbndTargetOperationType.read:
            assert op.columns is None
            assert op.columns_count is None
            assert op.dtypes is None
        if op.op_type == DbndTargetOperationType.write:
            assert op.columns == ["column_1", "column_2"]
            assert op.columns_count == 2
            assert op.dtypes == {"column_1": "integer", "column_2": "character varying"}


def test_copy_into_operation_failure(mock_redshift):
    expected = [
        RedshiftOperation(
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
            source_name="s3://test/test.json",
            target_name="FAIL",
        ),
        RedshiftOperation(
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
            source_name="s3://test/test.json",
            target_name="FAIL",
        ),
    ]
    with pytest.raises(Exception) as e:
        run_tracker_custom_query(COPY_INTO_TABLE_FAIL_QUERY, expected)
    assert str(e.value) == ERROR_MESSAGE


def test_copy_into_s3_graceful_failure(mock_redshift):
    expected = [
        RedshiftOperation(
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
            query="COPY \"schema_fail\" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
            source_name="s3://my/bucket/file.csv",
            target_name="schema_fail",
        ),
        RedshiftOperation(
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
            query="COPY \"schema_fail\" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
            source_name="s3://my/bucket/file.csv",
            target_name="schema_fail",
        ),
    ]
    return run_tracker_custom_query(
        COPY_INTO_TABLE_FROM_S3_FILE_GRACEFUL_FAIL_QUERY, expected
    )


def run_tracker_custom_query(query, expected):
    redshift_tracker = RedshiftTracker()

    with redshift_tracker as tracker:
        with _redshift_connect() as con:
            c = con.cursor()
            try:
                c.execute(query)
            finally:
                assert compare_eq_operations(
                    redshift_tracker.connections.get_operations(c), expected
                )
        # flush operations
    assert redshift_tracker.connections.get_operations(c) == []


def run_tracker_custom_df(query, dataframe=None):
    redshift_tracker = RedshiftTracker()
    with redshift_tracker as tracker:
        with _redshift_connect() as con:
            if dataframe is not None:
                tracker.set_dataframe(dataframe)
            c = con.cursor()
            c.execute(query)

            return tracker.connections.get_operations(con)
