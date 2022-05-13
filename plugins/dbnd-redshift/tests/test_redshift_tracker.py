import pandas as pd
import psycopg2
import pytest

from mock import Mock, patch
from mock.mock import MagicMock

from dbnd._core.constants import DbndTargetOperationType
from dbnd._core.utils.sql_tracker_common.sql_extract import Column
from dbnd_redshift.sdk.redshift_tracker import RedshiftTracker
from dbnd_redshift.sdk.redshift_utils import COPY_ROWS_COUNT_QUERY
from dbnd_redshift.sdk.redshift_values import (
    NUMERIC_TYPES,
    RedshiftOperation,
    get_col_stats_queries,
    query_list_to_text,
)


NON_NUMERIC_TYPES = [
    "char",
    "varchar(max)",
    "text",
    "boolean",
    "DATE",
    "TIME",
    "TIMETZ",
    "TIMESTAMP",
    "TIMESTAMPTZ",
]

NUMBER_OF_ROWS_INSERTED = 100

ERROR_MESSAGE = "Exception Mock"

COPY_INTO_TABLE_FROM_S3_FILE_QUERY = """COPY "MY_TABLE" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv"""
COPY_INTO_TABLE_FROM_S3_FILE_QUERY_SCHEMA = """COPY "MY_SCHEMA.MY_TABLE" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv"""
COPY_INTO_TABLE_FROM_S3_FILE_QUERY_COLUMNS = """COPY "MY_TABLE" (column_a, column_b) from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv"""
COPY_INTO_TABLE_FROM_S3_FILE_QUERY_SCHEMA_COLUMNS = """COPY "MY_SCHEMA.MY_TABLE" (column_a, column_b) from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv"""

COPY_INTO_TABLE_FAIL_QUERY = """copy into FAIL from s3://test/test.json CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');"""

COPY_INTO_TABLE_FROM_S3_FILE_GRACEFUL_FAIL_QUERY = """COPY "schema_fail" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv"""

GET_SCHEMA_FAIL_QUERY = "select * from pg_table_def where tablename='schema_fail'"


class CursorMock:
    def __init__(self, connection_mock):
        self.connection = connection_mock
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

    def fetchone(self):
        return tuple()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class ConnectionMock:
    def __init__(self):
        self.mock_cursor = CursorMock(self)

        dsn_parameters = {"host": "redshift.mock.test", "dbname": "mock_db"}

        self.closed = False

        self.info = MagicMock()
        self.info.options = ""
        self.info.host = "test"
        self.info.port = 12345
        self.info.dbname = "db"
        self.info.dsn_parameters.__getitem__.side_effect = dsn_parameters.__getitem__

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
    tests = [
        operation.extracted_schema == expected.extracted_schema,
        operation.query == expected.query,
        operation.source_name == expected.source_name,
        operation.database == expected.database,
        operation.schema_name == expected.schema_name,
        operation.table_name == expected.table_name,
        operation.op_type == expected.op_type,
    ]

    return all(tests)


def compare_eq_operations(
    operations: [RedshiftOperation], expected: [RedshiftOperation]
):
    # FIXME comparison
    # if len(operations) != len(expected):
    #     return False
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


def test_boolean_type_get_col_stats_queries():
    queries = get_col_stats_queries("column1", "boolean")
    for q in queries:
        if q.endswith("AS most_freq_value"):
            assert "integer::text" in q


@pytest.mark.parametrize("col_type", NUMERIC_TYPES + NON_NUMERIC_TYPES)
def test_get_type_agnostic_column_stats_queries(col_type):
    col_name = "column"
    most_freq_value_cast = "integer::text" if col_type == "boolean" else "text"
    queries = get_col_stats_queries(col_name, col_type)

    assert f"SELECT '{col_name}' AS name" in queries
    assert f"(SELECT COUNT(*) FROM DBND_TEMP) AS row_count" in queries
    assert (
        f'(SELECT COUNT(*) FROM DBND_TEMP where "{col_name}" is null) AS null_count'
        in queries
    )
    assert (
        f'(SELECT count(distinct "{col_name}") FROM DBND_TEMP) AS distinct_count'
        in queries
    )
    assert (
        f"""(SELECT top 1 count("{col_name}") FROM DBND_TEMP group by "{col_name}" order by count("{col_name}") DESC) AS most_freq_value_count"""
        in queries
    )
    assert (
        f"""(SELECT top 1 "{col_name}" FROM DBND_TEMP group by "{col_name}" order by count("{col_name}") DESC)::{most_freq_value_cast} AS most_freq_value"""
        in queries
    )


@pytest.mark.parametrize("col_type", NUMERIC_TYPES + ["decimal(10,5)", "numeric(3,14)"])
def test_get_col_stats_queries_stats_numeric_types(col_type):
    col_name = "column"
    queries = get_col_stats_queries(col_name, col_type)
    assert f'(SELECT stddev("{col_name}") FROM DBND_TEMP)::text AS stddev' in queries
    assert f'(SELECT avg("{col_name}") FROM DBND_TEMP)::text AS average' in queries
    assert f'(SELECT min("{col_name}") FROM DBND_TEMP)::text AS minimum' in queries
    assert f'(SELECT max("{col_name}") FROM DBND_TEMP)::text AS maximum' in queries
    assert (
        f'(SELECT percentile_cont(0.25) within group (order by "{col_name}" asc) from DBND_TEMP)::text AS percentile_25'
        in queries
    )
    assert (
        f'(SELECT percentile_cont(0.50) within group (order by "{col_name}" asc) from DBND_TEMP)::text AS percentile_50'
        in queries
    )
    assert (
        f'(SELECT percentile_cont(0.75) within group (order by "{col_name}" asc) from DBND_TEMP)::text AS percentile_75'
        in queries
    )


@pytest.mark.parametrize("col_type", NON_NUMERIC_TYPES)
def test_get_col_stats_queries_stats_other_types(col_type):
    col_name = "column"
    queries = get_col_stats_queries(col_name, col_type)

    assert "(SELECT Null) AS stddev" in queries
    assert "(SELECT Null) AS average" in queries
    assert "(SELECT Null) AS minimum" in queries
    assert "(SELECT Null) AS maximum" in queries
    assert "(SELECT Null) AS percentile_25" in queries
    assert "(SELECT Null) AS percentile_50" in queries
    assert "(SELECT Null) AS percentile_75" in queries


def test_extract_stats_query():
    queries = [
        list(get_col_stats_queries(col, col_type)) for col, col_type in schema.items()
    ]
    query = query_list_to_text(queries)
    assert all(k in query for k in schema)


def test_multiple_psycopg2_connections(mock_redshift):
    COPY_INTO_TABLE_FROM_S3_FILE_QUERY_2 = """COPY "MY_TABLE" from 's3://my/bucket/file2.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv"""

    expected1 = [
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
            query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY,
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
            dataframe=None,
            source_name="s3://my/bucket/file.csv",
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="my_table",
            cls_cache=None,
            schema_cache=None,
        ),
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
            query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY,
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
            dataframe=None,
            source_name="s3://my/bucket/file.csv",
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="my_table",
            cls_cache=None,
            schema_cache=None,
        ),
    ]
    expected2 = [
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
            query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY_2,
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="my_table",
            source_name="s3://my/bucket/file2.csv",
        ),
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
            query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY_2,
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="my_table",
            source_name="s3://my/bucket/file2.csv",
        ),
    ]

    redshift_tracker = RedshiftTracker()

    with redshift_tracker as tracker:
        with _redshift_connect() as con1, _redshift_connect() as con2:
            c1 = con1.cursor()
            c2 = con2.cursor()

            try:
                c1.execute(COPY_INTO_TABLE_FROM_S3_FILE_QUERY)
                c2.execute(COPY_INTO_TABLE_FROM_S3_FILE_QUERY_2)
            finally:
                if not compare_eq_operations(
                    redshift_tracker.connections.get_operations(c1), expected1
                ):
                    assert redshift_tracker.connections.get_operations(c1) == expected1
                if not compare_eq_operations(
                    redshift_tracker.connections.get_operations(c2), expected2
                ):
                    assert redshift_tracker.connections.get_operations(c2) == expected2
        # flush operations
    assert redshift_tracker.connections.get_operations(c1) == []
    assert redshift_tracker.connections.get_operations(c2) == []


def test_copy_into_s3(mock_redshift):
    expected = [
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
            query="""COPY "MY_TABLE" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv""",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="my_table",
            source_name="s3://my/bucket/file.csv",
        ),
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
            query="""COPY "MY_TABLE" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::12345:role/myRole' csv""",
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="my_table",
            source_name="s3://my/bucket/file.csv",
        ),
    ]
    return run_tracker_custom_query(COPY_INTO_TABLE_FROM_S3_FILE_QUERY, expected)


def test_copy_from_s3_schema_columns(mock_redshift):
    expected = [
        RedshiftOperation(
            extracted_schema={
                '"MY_SCHEMA.MY_TABLE".*': [
                    Column(
                        dataset_name='"MY_SCHEMA.MY_TABLE"',
                        name="*",
                        alias='"MY_SCHEMA.MY_TABLE".*',
                        is_file=False,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=NUMBER_OF_ROWS_INSERTED,
            query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY_SCHEMA_COLUMNS,
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
            host="redshift.mock.test",
            database="mock_db",
            schema_name="my_schema",
            table_name="my_table",
            source_name="s3://my/bucket/file.csv",
        ),
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
            query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY_SCHEMA_COLUMNS,
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
            host="redshift.mock.test",
            database="mock_db",
            schema_name="my_schema",
            table_name="my_table",
            source_name="s3://my/bucket/file.csv",
        ),
    ]
    return run_tracker_custom_query(
        COPY_INTO_TABLE_FROM_S3_FILE_QUERY_SCHEMA_COLUMNS, expected
    )


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
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="my_table",
            source_name="s3://test/test.json",
        ),
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
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="my_table",
            source_name="s3://test/test.json",
        ),
    ]
    with pytest.raises(Exception) as e:
        run_tracker_custom_query(COPY_INTO_TABLE_FAIL_QUERY, expected)
    assert str(e.value) == ERROR_MESSAGE


def test_copy_into_s3_graceful_failure(mock_redshift):
    expected = [
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
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="schema_fail",
            source_name="s3://my/bucket/file.csv",
        ),
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
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="schema_fail",
            source_name="s3://my/bucket/file.csv",
        ),
    ]
    return run_tracker_custom_query(
        COPY_INTO_TABLE_FROM_S3_FILE_GRACEFUL_FAIL_QUERY, expected
    )


def test_copy_into_with_columns(mock_redshift):
    expected = [
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
            records_count=100,
            query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY_COLUMNS,
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
            dataframe=None,
            source_name="s3://my/bucket/file.csv",
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="my_table",
            cls_cache=None,
            schema_cache=None,
        ),
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
            records_count=100,
            query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY_COLUMNS,
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
            dataframe=None,
            source_name="s3://my/bucket/file.csv",
            host="redshift.mock.test",
            database="mock_db",
            schema_name="public",
            table_name="my_table",
            cls_cache=None,
            schema_cache=None,
        ),
    ]
    run_tracker_custom_query(COPY_INTO_TABLE_FROM_S3_FILE_QUERY_COLUMNS, expected)


def test_copy_into_with_schema(mock_redshift):
    expected = [
        RedshiftOperation(
            extracted_schema={
                '"MY_SCHEMA.MY_TABLE".*': [
                    Column(
                        dataset_name='"MY_SCHEMA.MY_TABLE"',
                        name="*",
                        alias='"MY_SCHEMA.MY_TABLE".*',
                        is_file=False,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=100,
            query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY_SCHEMA,
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.write,
            error=None,
            dataframe=None,
            source_name="s3://my/bucket/file.csv",
            host="redshift.mock.test",
            database="mock_db",
            schema_name="my_schema",
            table_name="my_table",
            cls_cache=None,
            schema_cache=None,
        ),
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
            records_count=100,
            query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY_SCHEMA,
            query_id=None,
            success=True,
            op_type=DbndTargetOperationType.read,
            error=None,
            dataframe=None,
            source_name="s3://my/bucket/file.csv",
            host="redshift.mock.test",
            database="mock_db",
            schema_name="my_schema",
            table_name="my_table",
            cls_cache=None,
            schema_cache=None,
        ),
    ]
    run_tracker_custom_query(COPY_INTO_TABLE_FROM_S3_FILE_QUERY_SCHEMA, expected)


def run_tracker_custom_query(query, expected):
    redshift_tracker = RedshiftTracker()

    with redshift_tracker as tracker:
        with _redshift_connect() as con:
            c = con.cursor()
            try:
                c.execute(query)
                actual = redshift_tracker.connections.get_operations(c)
                if not compare_eq_operations(actual, expected):
                    for act, exp in zip(actual, expected):
                        assert act == exp
            except Exception as e:
                raise e
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
