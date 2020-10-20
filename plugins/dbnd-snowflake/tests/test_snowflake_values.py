from collections import OrderedDict
from textwrap import dedent

import mock

from pytest import fixture

from dbnd_snowflake.snowflake_values import (
    SnowflakeController,
    SnowflakeTable,
    SnowflakeTableValueType,
)
from targets.value_meta import ValueMetaConf


def snowflake_controller_mock():
    res = mock.MagicMock(SnowflakeController)
    res.get_column_types.return_value = {"name": "varchar"}
    res.get_dimensions.return_value = {"rows": 42, "cols": 12, "bytes": 500}
    res.to_preview.return_value = "test preview"
    res.return_value = res  # Mock constructor
    res.__enter__.return_value = res  # mock context manager
    return res


@fixture
def snowflake_conn_params():
    return {
        "user": "SNOWFLAKE_USER",
        "password": "SNOWFLAKE_PASSWORD",
        "account": "SNOWFLAKE_ACCOUNT",
    }


@fixture
def snowflake_conn_str(snowflake_conn_params):
    return "snowflake://{0[user]}:{0[password]}@{0[account]}".format(
        snowflake_conn_params
    )


@fixture
def snowflake_table(snowflake_conn_params):
    return SnowflakeTable(
        account=snowflake_conn_params["account"],
        user=snowflake_conn_params["user"],
        password=snowflake_conn_params["password"],
        database="SNOWFLAKE_SAMPLE_DATA",
        schema="TPCDS_SF100TCL",
        table_name="CUSTOMER",
    )


class TestSnowflakeTableValueType:
    def test_to_signature(self, snowflake_table):
        sign = SnowflakeTableValueType().to_signature(snowflake_table)

        assert (
            sign
            == "snowflake://SNOWFLAKE_USER:***@SNOWFLAKE_ACCOUNT/SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL/CUSTOMER"
        )

    def test_get_value_meta(self, snowflake_table):
        # Arrange
        with mock.patch(
            "dbnd_snowflake.snowflake_values.SnowflakeController",
            new_callable=snowflake_controller_mock,
        ) as snowflake:
            # Act
            value_meta = SnowflakeTableValueType().get_value_meta(
                snowflake_table, meta_conf=(ValueMetaConf.enabled())
            )

        # Assert
        assert value_meta.value_preview == "test preview"
        assert value_meta.data_dimensions == [42, 12]
        assert value_meta.data_schema == {
            "type": "SnowflakeTable",
            "column_types": {"name": "varchar"},
            "size": "500 B",
        }
        assert (
            value_meta.data_hash
            == "snowflake://SNOWFLAKE_USER:***@SNOWFLAKE_ACCOUNT/SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL/CUSTOMER"
        )
        assert snowflake.get_column_types.called
        assert snowflake.get_dimensions.called
        assert snowflake.to_preview.called


class TestSnowflakeController:
    def test_get_dimensions(self, snowflake_conn_str, snowflake_table):
        with mock.patch(
            "dbnd_snowflake.snowflake_values.SnowflakeController._query"
        ) as query_patch, mock.patch(
            "dbnd_snowflake.snowflake_values.SnowflakeController.get_column_types"
        ) as get_column_types_patch:
            # Arrange
            query_patch.side_effect = [[{"rows": 100, "bytes": 543}]]
            get_column_types_patch.side_effect = [{"foo": "varchar", "bar": "integer"}]
            # Act
            snowflake = SnowflakeController(snowflake_conn_str)
            dimensions = snowflake.get_dimensions(snowflake_table)

            # Assert
            query_patch.assert_has_calls(
                [
                    mock.call(
                        "SHOW TABLES LIKE 'CUSTOMER' in schema SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL"
                    ),
                ],
                any_order=True,
            )
            assert dimensions == {
                "rows": 100,
                "cols": 2,
                "bytes": 543,
            }

    def test_get_column_types(self, snowflake_conn_str, snowflake_table):
        with mock.patch(
            "dbnd_snowflake.snowflake_values.SnowflakeController._query"
        ) as query_patch:
            # Arrange
            query_patch.side_effect = [
                [{"COLUMN_NAME": "foo", "DATA_TYPE": "varchar"},],
            ]

            # Act
            snowflake = SnowflakeController(snowflake_conn_str)
            column_types = snowflake.get_column_types(snowflake_table)

            # Assert
            query_patch.assert_has_calls(
                [
                    mock.call(
                        "SELECT column_name, data_type\nFROM SNOWFLAKE_SAMPLE_DATA.information_schema.columns\nWHERE LOWER(table_name) = LOWER('CUSTOMER')\n    and LOWER(table_schema) = LOWER('TPCDS_SF100TCL')"
                    ),
                ],
                any_order=True,
            )
            assert column_types == {"foo": "varchar"}

    def test_to_preview(self, snowflake_conn_str, snowflake_table):
        with mock.patch(
            "dbnd_snowflake.snowflake_values.SnowflakeController._query"
        ) as prewview_query_patch, mock.patch(
            "dbnd_snowflake.snowflake_values.SnowflakeController.get_column_types"
        ) as get_column_types_patch:
            # Arrange
            get_column_types_patch.side_effect = [
                OrderedDict(foo="varchar", bar="integer")
            ]
            prewview_query_patch.side_effect = [[["1", "2"], ["2", "3"], ["3", "4"]]]
            # Act
            snowflake = SnowflakeController(snowflake_conn_str)
            preview = snowflake.to_preview(snowflake_table)

            # Assert
            prewview_query_patch.assert_has_calls(
                [
                    mock.call(
                        'select TRY_HEX_DECODE_STRING(HEX_ENCODE("foo")) AS foo,TRY_HEX_DECODE_STRING(HEX_ENCODE("bar")) AS bar from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER limit 20'
                    ),
                ],
                any_order=True,
            )
            assert preview == dedent(
                """\
                  0    1
                ---  ---
                  1    2
                  2    3
                  3    4
                ..."""
            )
