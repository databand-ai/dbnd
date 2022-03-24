import random

import pytest
import snowflake

from mock import Mock, patch
from snowflake.connector.cursor import SnowflakeCursor

from dbnd._core.constants import DbndTargetOperationType
from dbnd._core.sql_tracker_common.sql_extract import Column
from dbnd._core.sql_tracker_common.sql_operation import SqlOperation
from dbnd_snowflake.sdk.snowflake_tracker import SnowflakeTracker


SFQID = 12345

NUMBER_OF_ROWS_INSERTED = 10

RESULT_SET = {"data": {"stats": {"numRowsInserted": NUMBER_OF_ROWS_INSERTED}}}

ERROR_MESSAGE = "Exception Mock"

COPY_INTO_TABLE_FROM_S3_FILE_QUERY = """copy into TEST from s3://test/test.json CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');"""

COPY_INTO_TABLE_FULL_PATH_FROM_S3_FILE_QUERY = """copy into DATABASE.SCHEMA.TEST from s3://test/test.json CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');"""

COPY_INTO_TABLE_PARTIAL_PATH_FROM_S3_FILE_QUERY = """copy into SCHEMA.TEST from s3://test/test.json CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');"""

COPY_INTO_TABLE_WITH_COLUMNS_FROM_S3_FILE_QUERY = """copy into TEST ("column_a", "column_b") from s3://test/test.json CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');"""

COPY_INTO_TABLE_FULL_PATH_WITH_COLUMNS_FROM_S3_FILE_QUERY = """copy into DATABASE.SCHEMA.TEST ("column_a", "column_b") from s3://test/test.json CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');"""

COPY_INTO_TABLE_PARTIAL_PATH_WITH_COLUMNS_FROM_S3_FILE_QUERY = """copy into SCHEMA.TEST ("column_a", "column_b") from s3://test/test.json CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');"""

COPY_INTO_TABLE_FROM_STAGE_FILE_QUERY = """copy into TEST from @STAGE;"""

COPY_INTO_TABLE_FULL_PATH_FROM_STAGE_FILE_FULL_PATH_QUERY = (
    """copy into DATABASE.SCHEMA.TEST from DATABASE.SCHEMA.TEST.@STAGE;"""
)

COPY_INTO_TABLE_PARTIAL_PATH_FROM_STAGE_FILE_PARTIAL_PATH_QUERY = (
    """copy into SCHEMA.TEST from SCHEMA.@STAGE;"""
)

COPY_INTO_TABLE_WITH_COLUMNS_FROM_STAGE_FILE_QUERY = (
    """copy into TEST ("column_a", "column_b") from @STAGE;"""
)

COPY_INTO_TABLE_WITH_COLUMNS_FULL_PATH_FROM_STAGE_FILE_FULL_PATH_QUERY = """copy into DATABASE.SCHEMA.TEST ("column_a", "column_b") from DATABASE.SCHEMA.TEST.@STAGE;"""

COPY_INTO_TABLE_WITH_COLUMNS_PARTIAL_PATH_FROM_STAGE_FILE_PARTIAL_PATH_QUERY = (
    """copy into SCHEMA.TEST ("column_a", "column_b") from SCHEMA.TEST.@STAGE;"""
)

COPY_INTO_TABLE_FROM_S3_FILE_APOSTROPHE_QUERY = """copy into TEST from 's3://test/test.json' CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');"""

COPY_INTO_TABLE_FROM_S3_FILE_QUOTES_QUERY = """copy into TEST from "s3://test/test.json" CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');"""

COPY_INTO_TABLE_FROM_STAGE_FILE_APOSTROPHE_QUERY = """copy into TEST from '@STAGE';"""

COPY_INTO_TABLE_FROM_STAGE_FILE_QUOTES_QUERY = """copy into TEST from "@STAGE";"""

COPY_INTO_TABLE_WITH_NESTED_QUERY_WITH_COLUMNS_FROM_STAGE_FILE_QUERY = (
    """copy into TEST from (select "column_a", "column_b" from @STAGE);"""
)

COPY_INTO_TABLE_FAIL_QUERY = """copy into FAIL from s3://test/test.json CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');"""


def _authenticate(self_auth, *args, **kwargs):
    self_auth._rest._connection._session_id = random.randint(0, 2000000000)
    return {}


@pytest.fixture
def mock_snowflake():
    execute_mock = Mock()

    def __execute_helper(query, *args, **kwargs):
        # type: (str, ..., ...) -> dict
        return RESULT_SET

    def _execute(self_cursor, command, *args, **kwargs):
        # type: (SnowflakeCursor, str, ..., ...) -> SnowflakeCursor
        execute_mock(command, *args, **kwargs)
        self_cursor._sfqid = SFQID
        if "FAIL" in command:
            raise Exception(ERROR_MESSAGE)
        # call execute _helper to mock number of rows inserted
        self_cursor._execute_helper()
        if "desc" in command:
            result = [
                {"name": "column_a", "type": "int"},
                {"name": "column_b", "type": "int"},
            ]
            self_cursor._result = (x for x in result)
        return self_cursor

    with patch.object(
        snowflake.connector.auth.Auth, "authenticate", new=_authenticate
    ), patch.object(
        snowflake.connector.cursor.SnowflakeCursor, "execute", new=_execute
    ), patch.object(
        snowflake.connector.cursor.SnowflakeCursor,
        "_execute_helper",
        new=__execute_helper,
    ), patch.object(
        snowflake.connector.network.SnowflakeRestful, "delete_session"
    ):
        yield execute_mock


def _snowflake_connect():
    return snowflake.connector.connect(
        user="test_user",
        password="test_password",
        account="test_account",
        database="test_database",
        schema="test_schema",
    )


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            COPY_INTO_TABLE_FROM_S3_FILE_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "s3://test/test.json.*": [
                            Column(
                                dataset_name="s3://test/test.json",
                                alias="s3://test/test.json.*",
                                name="*",
                                is_file=True,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "TEST.*": [
                            Column(
                                dataset_name="TEST",
                                alias="TEST.*",
                                name="*",
                                is_file=False,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_WITH_COLUMNS_FROM_S3_FILE_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "s3://test/test.json.*": [
                            Column(
                                dataset_name="s3://test/test.json",
                                alias="s3://test/test.json.*",
                                name="*",
                                is_file=True,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_WITH_COLUMNS_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "column_a": [
                            Column(
                                dataset_name="TEST",
                                alias="column_a",
                                name="column_a",
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                        "column_b": [
                            Column(
                                dataset_name="TEST",
                                alias="column_b",
                                name="column_b",
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_WITH_COLUMNS_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_FULL_PATH_FROM_S3_FILE_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "s3://test/test.json.*": [
                            Column(
                                dataset_name="s3://test/test.json",
                                alias="s3://test/test.json.*",
                                name="*",
                                is_file=True,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FULL_PATH_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "DATABASE.SCHEMA.TEST.*": [
                            Column(
                                dataset_name="DATABASE.SCHEMA.TEST",
                                alias="DATABASE.SCHEMA.TEST.*",
                                name="*",
                                is_file=False,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FULL_PATH_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_FULL_PATH_WITH_COLUMNS_FROM_S3_FILE_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "s3://test/test.json.*": [
                            Column(
                                dataset_name="s3://test/test.json",
                                alias="s3://test/test.json.*",
                                name="*",
                                is_file=True,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FULL_PATH_WITH_COLUMNS_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        '"column_a"': [
                            Column(
                                dataset_name="TEST",
                                name='"column_a"',
                                alias='"column_a"',
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                        '"column_b"': [
                            Column(
                                dataset_name="TEST",
                                name='"column_b"',
                                alias='"column_b"',
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FULL_PATH_WITH_COLUMNS_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_PARTIAL_PATH_FROM_S3_FILE_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "s3://test/test.json.*": [
                            Column(
                                dataset_name="s3://test/test.json",
                                alias="s3://test/test.json.*",
                                name="*",
                                is_file=True,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_PARTIAL_PATH_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "SCHEMA.TEST.*": [
                            Column(
                                dataset_name="SCHEMA.TEST",
                                alias="SCHEMA.TEST.*",
                                name="*",
                                is_file=False,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_PARTIAL_PATH_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_PARTIAL_PATH_WITH_COLUMNS_FROM_S3_FILE_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "s3://test/test.json.*": [
                            Column(
                                dataset_name="s3://test/test.json",
                                alias="s3://test/test.json.*",
                                name="*",
                                is_file=True,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_PARTIAL_PATH_WITH_COLUMNS_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        '"column_a"': [
                            Column(
                                dataset_name="TEST",
                                name='"column_a"',
                                alias='"column_a"',
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                        '"column_b"': [
                            Column(
                                dataset_name="TEST",
                                name='"column_b"',
                                alias='"column_b"',
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_PARTIAL_PATH_WITH_COLUMNS_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_FROM_S3_FILE_APOSTROPHE_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "s3://test/test.json.*": [
                            Column(
                                dataset_name="s3://test/test.json",
                                alias="s3://test/test.json.*",
                                name="*",
                                is_file=True,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "TEST.*": [
                            Column(
                                dataset_name="TEST",
                                alias="TEST.*",
                                name="*",
                                is_file=False,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_FROM_S3_FILE_QUOTES_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "s3://test/test.json.*": [
                            Column(
                                dataset_name="s3://test/test.json",
                                alias="s3://test/test.json.*",
                                name="*",
                                is_file=True,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "TEST.*": [
                            Column(
                                dataset_name="TEST",
                                alias="TEST.*",
                                name="*",
                                is_file=False,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_S3_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
    ],
)
def test_copy_into_s3(mock_snowflake, query, expected):
    return run_tracker_custom_query(mock_snowflake, query, expected)


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            COPY_INTO_TABLE_FROM_STAGE_FILE_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "@STAGE.*": [
                            Column(
                                dataset_name="@STAGE",
                                alias="@STAGE.*",
                                name="*",
                                is_file=False,
                                is_stage=True,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_STAGE_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "TEST.*": [
                            Column(
                                dataset_name="TEST",
                                alias="TEST.*",
                                name="*",
                                is_file=False,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_STAGE_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_WITH_COLUMNS_FROM_STAGE_FILE_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "@STAGE.*": [
                            Column(
                                dataset_name="@STAGE",
                                alias="@STAGE.*",
                                name="*",
                                is_file=False,
                                is_stage=True,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_WITH_COLUMNS_FROM_STAGE_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "column_a": [
                            Column(
                                dataset_name="TEST",
                                alias="column_a",
                                name="column_a",
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                        "column_b": [
                            Column(
                                dataset_name="TEST",
                                alias="column_b",
                                name="column_b",
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_WITH_COLUMNS_FROM_STAGE_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_FULL_PATH_FROM_STAGE_FILE_FULL_PATH_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "DATABASE.SCHEMA.TEST.@STAGE.*": [
                            Column(
                                dataset_name="DATABASE.SCHEMA.TEST.@STAGE",
                                alias="DATABASE.SCHEMA.TEST.@STAGE.*",
                                name="*",
                                is_file=False,
                                is_stage=True,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FULL_PATH_FROM_STAGE_FILE_FULL_PATH_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "DATABASE.SCHEMA.TEST.*": [
                            Column(
                                dataset_name="DATABASE.SCHEMA.TEST",
                                alias="DATABASE.SCHEMA.TEST.*",
                                name="*",
                                is_file=False,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FULL_PATH_FROM_STAGE_FILE_FULL_PATH_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_PARTIAL_PATH_FROM_STAGE_FILE_PARTIAL_PATH_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "SCHEMA.@STAGE.*": [
                            Column(
                                dataset_name="SCHEMA.@STAGE",
                                alias="SCHEMA.@STAGE.*",
                                name="*",
                                is_file=False,
                                is_stage=True,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_PARTIAL_PATH_FROM_STAGE_FILE_PARTIAL_PATH_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "SCHEMA.TEST.*": [
                            Column(
                                dataset_name="SCHEMA.TEST",
                                alias="SCHEMA.TEST.*",
                                name="*",
                                is_file=False,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_PARTIAL_PATH_FROM_STAGE_FILE_PARTIAL_PATH_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_WITH_COLUMNS_FULL_PATH_FROM_STAGE_FILE_FULL_PATH_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "DATABASE.SCHEMA.TEST.@STAGE.*": [
                            Column(
                                dataset_name="DATABASE.SCHEMA.TEST.@STAGE",
                                alias="DATABASE.SCHEMA.TEST.@STAGE.*",
                                name="*",
                                is_file=False,
                                is_stage=True,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_WITH_COLUMNS_FULL_PATH_FROM_STAGE_FILE_FULL_PATH_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        '"column_a"': [
                            Column(
                                dataset_name="TEST",
                                name='"column_a"',
                                alias='"column_a"',
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                        '"column_b"': [
                            Column(
                                dataset_name="TEST",
                                name='"column_b"',
                                alias='"column_b"',
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_WITH_COLUMNS_FULL_PATH_FROM_STAGE_FILE_FULL_PATH_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_WITH_COLUMNS_PARTIAL_PATH_FROM_STAGE_FILE_PARTIAL_PATH_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "SCHEMA.TEST.@STAGE.*": [
                            Column(
                                dataset_name="SCHEMA.TEST.@STAGE",
                                alias="SCHEMA.TEST.@STAGE.*",
                                name="*",
                                is_file=False,
                                is_stage=True,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_WITH_COLUMNS_PARTIAL_PATH_FROM_STAGE_FILE_PARTIAL_PATH_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        '"column_a"': [
                            Column(
                                dataset_name="TEST",
                                name='"column_a"',
                                alias='"column_a"',
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                        '"column_b"': [
                            Column(
                                dataset_name="TEST",
                                name='"column_b"',
                                alias='"column_b"',
                                is_file=False,
                                is_stage=False,
                            )
                        ],
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_WITH_COLUMNS_PARTIAL_PATH_FROM_STAGE_FILE_PARTIAL_PATH_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_FROM_STAGE_FILE_APOSTROPHE_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "@STAGE.*": [
                            Column(
                                dataset_name="@STAGE",
                                alias="@STAGE.*",
                                name="*",
                                is_file=False,
                                is_stage=True,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_STAGE_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "TEST.*": [
                            Column(
                                dataset_name="TEST",
                                alias="TEST.*",
                                name="*",
                                is_file=False,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_STAGE_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_FROM_STAGE_FILE_QUOTES_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "@STAGE.*": [
                            Column(
                                dataset_name="@STAGE",
                                alias="@STAGE.*",
                                name="*",
                                is_file=False,
                                is_stage=True,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_STAGE_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "TEST.*": [
                            Column(
                                dataset_name="TEST",
                                alias="TEST.*",
                                name="*",
                                is_file=False,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_FROM_STAGE_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
        (
            COPY_INTO_TABLE_WITH_NESTED_QUERY_WITH_COLUMNS_FROM_STAGE_FILE_QUERY,
            [
                SqlOperation(
                    extracted_schema={
                        "@STAGE.*": [
                            Column(
                                dataset_name="@STAGE",
                                alias="@STAGE.*",
                                name="*",
                                is_file=False,
                                is_stage=True,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_WITH_NESTED_QUERY_WITH_COLUMNS_FROM_STAGE_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.read,
                    error=None,
                ),
                SqlOperation(
                    extracted_schema={
                        "TEST.*": [
                            Column(
                                dataset_name="TEST",
                                alias="TEST.*",
                                name="*",
                                is_file=False,
                                is_stage=False,
                            )
                        ]
                    },
                    dtypes=None,
                    records_count=NUMBER_OF_ROWS_INSERTED,
                    query=COPY_INTO_TABLE_WITH_NESTED_QUERY_WITH_COLUMNS_FROM_STAGE_FILE_QUERY,
                    query_id=SFQID,
                    success=True,
                    op_type=DbndTargetOperationType.write,
                    error=None,
                ),
            ],
        ),
    ],
)
def test_copy_into_stage(mock_snowflake, query, expected):
    return run_tracker_custom_query(mock_snowflake, query, expected)


def test_copy_into_failure(mock_snowflake):
    expected = [
        SqlOperation(
            extracted_schema={
                "s3://test/test.json.*": [
                    Column(
                        dataset_name="s3://test/test.json",
                        alias="s3://test/test.json.*",
                        name="*",
                        is_file=True,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=0,
            query=COPY_INTO_TABLE_FAIL_QUERY,
            query_id=SFQID,
            success=False,
            op_type=DbndTargetOperationType.read,
            error=ERROR_MESSAGE,
        ),
        SqlOperation(
            extracted_schema={
                "FAIL.*": [
                    Column(
                        dataset_name="FAIL",
                        alias="FAIL.*",
                        name="*",
                        is_file=False,
                        is_stage=False,
                    )
                ]
            },
            dtypes=None,
            records_count=0,
            query=COPY_INTO_TABLE_FAIL_QUERY,
            query_id=SFQID,
            success=False,
            op_type=DbndTargetOperationType.write,
            error=ERROR_MESSAGE,
        ),
    ]
    with pytest.raises(Exception) as e:
        run_tracker_custom_query(mock_snowflake, COPY_INTO_TABLE_FAIL_QUERY, expected)
    assert str(e.value) == ERROR_MESSAGE


def run_tracker_custom_query(mock_snowflake, query, expected):
    snowflake_connection = _snowflake_connect()
    snowflake_tracker = SnowflakeTracker()
    expected_result_set = RESULT_SET
    with snowflake_tracker:
        with snowflake_connection as con:
            c = con.cursor()
            try:
                c.execute(query)
            except Exception as e:
                expected_result_set = None
                raise e
            finally:
                assert snowflake_tracker._connection == snowflake_connection
                assert snowflake_tracker.operations == expected
                assert snowflake_tracker.result_set == expected_result_set
        # flush operations
    assert snowflake_tracker.operations == []
