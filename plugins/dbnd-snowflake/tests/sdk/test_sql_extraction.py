import pytest
import sqlparse

from dbnd._core.constants import DbndTargetOperationType
from dbnd_snowflake.sdk.sql_extract import Column, SqlQueryExtractor


def parse_first_query(sqlquery):
    return sqlparse.parse(sqlquery)[0]


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
copy into JSON_TABLE from s3://bucket/file.json CREDENTIALS = (AWS_KEY_ID = '12345' AWS_SECRET_KEY = '12345')
        """,
            {
                DbndTargetOperationType.read: {
                    "s3://bucket/file.json.*": [
                        Column(
                            dataset_name="s3://bucket/file.json",
                            name="*",
                            alias="s3://bucket/file.json.*",
                            is_file=True,
                        )
                    ]
                },
                DbndTargetOperationType.write: {
                    "JSON_TABLE.*": [
                        Column(
                            dataset_name="JSON_TABLE",
                            name="*",
                            alias="JSON_TABLE.*",
                            is_file=False,
                            is_stage=False,
                        )
                    ]
                },
            },
        )
    ],
)
def test_copy_into_from_external_file(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
copy into JSON_TABLE ("Company Name", "Description") from s3://bucket/file.json CREDENTIALS = (AWS_KEY_ID = '12345' AWS_SECRET_KEY = '12345')
        """,
            {
                DbndTargetOperationType.read: {
                    "s3://bucket/file.json.*": [
                        Column(
                            dataset_name="s3://bucket/file.json",
                            name="*",
                            alias="s3://bucket/file.json.*",
                            is_file=True,
                            is_stage=False,
                        )
                    ]
                },
                DbndTargetOperationType.write: {
                    "Company Name": [
                        Column(
                            dataset_name="JSON_TABLE",
                            name="Company Name",
                            alias="Company Name",
                            is_file=False,
                            is_stage=False,
                        )
                    ],
                    "Description": [
                        Column(
                            dataset_name="JSON_TABLE",
                            name="Description",
                            alias="Description",
                            is_file=False,
                            is_stage=False,
                        )
                    ],
                },
            },
        )
    ],
)
def test_copy_into_with_columns_from_external_file(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
copy into JSON_TABLE from @STAGE
        """,
            {
                DbndTargetOperationType.read: {
                    "@STAGE.*": [
                        Column(
                            dataset_name="@STAGE",
                            name="*",
                            alias="@STAGE.*",
                            is_file=False,
                            is_stage=True,
                        )
                    ]
                },
                DbndTargetOperationType.write: {
                    "JSON_TABLE.*": [
                        Column(
                            dataset_name="JSON_TABLE",
                            name="*",
                            alias="JSON_TABLE.*",
                            is_file=False,
                            is_stage=False,
                        )
                    ]
                },
            },
        )
    ],
)
def test_copy_into_from_stage(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
copy into JSON_TABLE ("Company Name", "Description") from @STAGE
        """,
            {
                DbndTargetOperationType.read: {
                    "@STAGE.*": [
                        Column(
                            dataset_name="@STAGE",
                            name="*",
                            alias="@STAGE.*",
                            is_file=False,
                            is_stage=True,
                        )
                    ]
                },
                DbndTargetOperationType.write: {
                    "Company Name": [
                        Column(
                            dataset_name="JSON_TABLE",
                            name="Company Name",
                            alias="Company Name",
                            is_file=False,
                            is_stage=False,
                        )
                    ],
                    "Description": [
                        Column(
                            dataset_name="JSON_TABLE",
                            name="Description",
                            alias="Description",
                            is_file=False,
                            is_stage=False,
                        )
                    ],
                },
            },
        )
    ],
)
def test_copy_into_with_columns_from_stage(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
    SELECT * FROM TABLE
            """,
            {},
        )
    ],
)
def test_out_of_scope_query(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
    SELECT FROM FROM TABLE WHERE FOO BAR
            """,
            {},
        )
    ],
)
def test_invalid_query(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
    COPY INTO TEST FROM "s3://some/url/to/file.txt" CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');
            """,
            "COPY INTO TEST FROM s3://some/url/to/file.txt CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');",
        ),
        (
            """
    COPY INTO TEST FROM 's3://some/url/to/file.txt' CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');
            """,
            "COPY INTO TEST FROM s3://some/url/to/file.txt CREDENTIALS = (AWS_KEY_ID = 'test' AWS_SECRET_KEY = 'test');",
        ),
        (
            """
    COPY INTO TEST FROM "@stage";
            """,
            "COPY INTO TEST FROM @stage;",
        ),
        (
            """
    COPY INTO TEST FROM '@stage';
            """,
            "COPY INTO TEST FROM @stage;",
        ),
    ],
)
def test_clean_query(sqlquery, expected):
    assert SqlQueryExtractor().clean_query(sqlquery) == expected
