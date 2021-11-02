import pytest

from dbnd_snowflake.POC.tracker import extract_schema_from_sf_desc


@pytest.mark.parametrize(
    "description, expected",
    [
        ([("UNIQUE_KEY", 0, None, None, 38, 0, True),], {"UNIQUE_KEY": "NUMBER"},),
        (
            [
                ("UNIQUE_KEY", 0, None, None, 38, 0, True),
                ("CREATED_DATE", 8, None, None, 0, 9, True),
                ("PARK_BOROUGH", 2, None, 16777216, None, None, True),
                ("DUE_DATE", 8, None, None, 0, 9, True),
                ("load_date", 3, None, None, None, None, True),
                ("Opening_Price", 1, None, None, None, None, True),
            ],
            {
                "CREATED_DATE": "TIMESTAMP_TZ",
                "DUE_DATE": "TIMESTAMP_TZ",
                "Opening_Price": "FLOAT",
                "PARK_BOROUGH": "VARCHAR",
                "UNIQUE_KEY": "NUMBER",
                "load_date": "DATE",
            },
        ),
    ],
)
def test_get_schema(description, expected):
    assert extract_schema_from_sf_desc(description) == expected
