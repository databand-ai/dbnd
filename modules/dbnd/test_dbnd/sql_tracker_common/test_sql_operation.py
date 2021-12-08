import pytest

from mock import MagicMock

from dbnd._core.constants import DbndTargetOperationType
from dbnd._core.sql_tracker_common import sql_operation
from dbnd._core.sql_tracker_common.sql_extract import Column
from dbnd._core.sql_tracker_common.sql_operation import SqlOperation


def generate_snowflake_connection_mock():
    snowflake_connection_mock = MagicMock()
    snowflake_connection_mock.host = "www.snowflake.mock.com"
    snowflake_connection_mock.database = "mock"
    snowflake_connection_mock.schema = "test"
    snowflake_connection_mock.port = 1234
    return snowflake_connection_mock


def generate_sql_operation_mock(op_type, table, is_file, is_stage):
    return SqlOperation(
        extracted_schema={
            table: [
                Column(
                    dataset_name=table,
                    alias=table,
                    name="*",
                    is_file=is_file,
                    is_stage=is_stage,
                )
            ]
        },
        dtypes=None,
        records_count=1,
        query="",
        query_id=1,
        success=True,
        op_type=op_type,
        error=None,
    )


@pytest.mark.parametrize(
    "operation, expected",
    [
        (
            generate_sql_operation_mock(
                DbndTargetOperationType.read, "TABLE", is_file=False, is_stage=False
            ),
            "snowflake://www.snowflake.mock.com:1234/mock/test/TABLE",
        ),
        (
            generate_sql_operation_mock(
                DbndTargetOperationType.read,
                "s3://path/to/my/file.csv",
                is_file=True,
                is_stage=False,
            ),
            "s3://path/to/my/file.csv",
        ),
        (
            generate_sql_operation_mock(
                DbndTargetOperationType.read, "@STAGE", is_file=False, is_stage=True
            ),
            "snowflake://www.snowflake.mock.com:1234/mock/test/@STAGE",
        ),
        (
            generate_sql_operation_mock(
                DbndTargetOperationType.write, "TABLE", is_file=False, is_stage=False
            ),
            "snowflake://www.snowflake.mock.com:1234/mock/test/TABLE",
        ),
    ],
)
def test_render_connection_path(operation, expected):
    assert (
        sql_operation.render_connection_path(
            connection=generate_snowflake_connection_mock(),
            operation=operation,
            conn_type="snowflake",
        )
        == expected
    )
