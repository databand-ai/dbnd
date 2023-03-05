# © Copyright Databand.ai, an IBM Company 2022

from collections import OrderedDict
from textwrap import dedent

import mock

from pytest import fixture

from dbnd_snowflake.snowflake_controller import SnowflakeController
from dbnd_snowflake.snowflake_values import SnowflakeTable, SnowflakeTableValueType
from targets.value_meta import ValueMetaConf


EXPECTED_SNOWFLAKE_TABLE_SIGNATURE = "snowflake://SNOWFLAKE_USER:***@snowflake_account/SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL/CUSTOMER"

EXPECTED_SNOWFLAKE_TABLE_HASH = str(hash(EXPECTED_SNOWFLAKE_TABLE_SIGNATURE))


@fixture
def snowflake_conn_params():
    return {
        "user": "SNOWFLAKE_USER",
        "password": "SNOWFLAKE_PASSWORD",  # pragma: allowlist secret
        "account": "SNOWFLAKE_ACCOUNT",
    }


@fixture
def snowflake_conn_str(snowflake_conn_params):
    return "snowflake://{0[user]}:{0[password]}@{0[account]}".format(  # gitleaks:allow
        snowflake_conn_params
    )


@fixture
def snowflake_controller_mock(snowflake_conn_str):
    ctrl = SnowflakeController(snowflake_conn_str)
    res = mock.MagicMock(ctrl, wraps=ctrl)
    res.get_column_types = mock.MagicMock(return_value={"name": "varchar"})
    res.get_dimensions = mock.MagicMock(
        return_value={"rows": 42, "cols": 12, "bytes": 500}
    )
    res.to_preview = mock.MagicMock(return_value="test preview")
    # res.return_value = res  # Mock constructor
    res.__enter__ = mock.MagicMock(return_value=res)  # mock context manager
    res.__str__ = mock.Mock(return_value=ctrl.__str__())
    return res


@fixture
def snowflake_table(snowflake_controller_mock):
    return SnowflakeTable(
        snowflake_ctrl=snowflake_controller_mock,
        database="SNOWFLAKE_SAMPLE_DATA",
        schema='"TPCDS_SF100TCL"',
        table_name='"CUSTOMER"',
    )


class TestSnowflakeTableValueType:
    def test_to_signature(self, snowflake_table):
        sign = SnowflakeTableValueType().to_signature(snowflake_table)

        assert sign == EXPECTED_SNOWFLAKE_TABLE_SIGNATURE

    def test_get_value_meta(self, snowflake_table):
        value_meta = SnowflakeTableValueType().get_value_meta(
            snowflake_table, meta_conf=(ValueMetaConf.enabled())
        )

        # Assert
        assert value_meta.value_preview == "test preview"
        assert value_meta.data_dimensions == [42, 12]
        assert value_meta.data_schema.as_dict() == {
            "type": "SnowflakeTable",
            "dtypes": {"name": "varchar"},
            "size.bytes": 500,
        }
        assert value_meta.data_hash == EXPECTED_SNOWFLAKE_TABLE_HASH
        assert snowflake_table.snowflake_ctrl.get_column_types.called
        assert snowflake_table.snowflake_ctrl.get_dimensions.called
        assert snowflake_table.snowflake_ctrl.to_preview.called

    def test_get_value_meta_empty(self, snowflake_table):
        value_meta = SnowflakeTableValueType().get_value_meta(
            snowflake_table,
            meta_conf=(
                ValueMetaConf(log_preview=False, log_schema=False, log_size=False)
            ),
        )

        # Assert
        assert value_meta.value_preview is None
        assert value_meta.data_dimensions is None
        assert value_meta.data_schema == {}
        assert value_meta.data_hash == EXPECTED_SNOWFLAKE_TABLE_HASH
        assert not snowflake_table.snowflake_ctrl.get_column_types.called
        assert not snowflake_table.snowflake_ctrl.get_dimensions.called
        assert not snowflake_table.snowflake_ctrl.to_preview.called


class TestSnowflakeController:
    def test_get_dimensions(self, snowflake_conn_str, snowflake_table):
        # Arrange
        with mock.patch(
            "dbnd_snowflake.snowflake_controller.SnowflakeController.query",
            return_value=[{"rows": 100, "bytes": 543}],
        ) as query_patch, mock.patch(
            "dbnd_snowflake.snowflake_controller.SnowflakeController.get_column_types",
            return_value={"foo": "varchar", "bar": "integer"},
        ):
            # Act
            snowflake = SnowflakeController(snowflake_conn_str)
            dimensions = snowflake.get_dimensions(snowflake_table)

            # Assert
            query_patch.assert_has_calls(
                [
                    mock.call(
                        'SHOW TABLES LIKE \'CUSTOMER\' in schema "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"'
                    )
                ],
                any_order=True,
            )
            assert dimensions == {"rows": 100, "cols": 2, "bytes": 543}

    def test_get_column_types(self, snowflake_conn_str, snowflake_table):
        # Arrange
        with mock.patch(
            "dbnd_snowflake.snowflake_controller.SnowflakeController.query",
            return_value=[{"COLUMN_NAME": "foo", "DATA_TYPE": "varchar"}],
        ) as query_patch:
            # Act
            snowflake = SnowflakeController(snowflake_conn_str)
            column_types = snowflake.get_column_types(snowflake_table)

            # Assert
            query_patch.assert_has_calls(
                [
                    mock.call(
                        "SELECT column_name, data_type\n"
                        'FROM "SNOWFLAKE_SAMPLE_DATA".information_schema.columns\n'
                        "WHERE LOWER(table_name) = LOWER('CUSTOMER') and LOWER(table_schema) = LOWER('TPCDS_SF100TCL')"
                    )
                ],
                any_order=True,
            )
            assert column_types == {"foo": "varchar"}

    def test_to_preview(self, snowflake_conn_str, snowflake_table):
        # Arrange
        with mock.patch(
            "dbnd_snowflake.snowflake_controller.SnowflakeController.query",
            return_value=[["1", "2"], ["2", "3"], ["3", "4"]],
        ) as preview_query_patch, mock.patch(
            "dbnd_snowflake.snowflake_controller.SnowflakeController.get_column_types",
            return_value=OrderedDict(foo="varchar", bar="integer"),
        ):
            # Act
            snowflake = SnowflakeController(snowflake_conn_str)
            preview = snowflake.to_preview(snowflake_table)

            # Assert
            preview_query_patch.assert_has_calls(
                [
                    mock.call(
                        'select TRY_HEX_DECODE_STRING(HEX_ENCODE("foo")) AS foo,TRY_HEX_DECODE_STRING(HEX_ENCODE("bar")) AS bar from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" limit 20'
                    )
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
