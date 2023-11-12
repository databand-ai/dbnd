# © Copyright Databand.ai, an IBM Company 2022
import logging

import pandas as pd
import pytest

from mock.mock import Mock

from dbnd._core.constants import DbndDatasetOperationType
from dbnd_redshift.sdk.redshift_values import RedshiftOperation, RedshiftTableValueType
from targets.data_schema import DataSchemaArgs
from targets.value_meta import ValueMetaConf


COPY_FROM_S3_FILE_QUERY = """COPY "MY_TABLE" from 's3://my/bucket/file.csv' iam_role 'arn:aws:iam::***:role/myRole' csv"""
REDSHIFT_VALUE_PREVIEW = """\
   Names  Births  Married
     Bob     968     True
 Jessica     155    False
    Mary      77     True
    John     578    False
     Mel     973     True"""

STUB_COLUMN_STATS = Mock()

STUB_PANDAS_DATAFRAME = pd.DataFrame(
    {
        "Names": pd.Series(["Bob", "Jessica", "Mary", "John", "Mel"], dtype="str"),
        "Births": pd.Series([968, 155, 77, 578, 973], dtype="int"),
        "Married": pd.Series([True, False, True, False, True], dtype="bool"),
    }
)


class TestDataFrameValueType(object):
    @pytest.mark.parametrize(
        "meta_conf, expected_data_dimensions, expected_data_schema, expected_column_stats, expected_value_preview",
        [
            [
                ValueMetaConf(log_preview=True, log_schema=False),
                (100, None),
                None,
                {},
                REDSHIFT_VALUE_PREVIEW,
            ],
            [
                ValueMetaConf(log_preview=True, log_schema=True),
                (3, 5),
                DataSchemaArgs(
                    type="RedshiftOperation",
                    columns_names=["Names", "Births", "Married"],
                    columns_types={},
                    shape=(3, 5),
                    byte_size=0,
                ),
                {},
                REDSHIFT_VALUE_PREVIEW,
            ],
            [ValueMetaConf(log_stats=True), (100, None), None, STUB_COLUMN_STATS, ""],
        ],
    )
    def test_df_value_meta(
        self,
        meta_conf: ValueMetaConf,
        expected_data_dimensions,
        expected_data_schema,
        expected_column_stats,
        expected_value_preview,
    ):
        # Arrange
        schema = {
            "type": "RedshiftOperation",
            "columns": ["Names", "Births", "Married"],
            "dtypes": {},
            "shape": (3, 5),
            "size.bytes": 0,
        }

        redshift_operation = RedshiftOperation(
            records_count=100,
            query=COPY_FROM_S3_FILE_QUERY,
            dataframe=None,
            schema_cache=schema,
            preview_cache=STUB_PANDAS_DATAFRAME,
            cls_cache=STUB_COLUMN_STATS,
            # Not used in this test, hence empty
            extracted_schema={},
            dtypes={},
            query_id="",
            success=True,
            op_type=DbndDatasetOperationType.write,
            error="",
        )

        # Act
        redshift_value_meta = RedshiftTableValueType().get_value_meta(
            redshift_operation, meta_conf
        )

        # Assert
        assert redshift_value_meta.data_dimensions == expected_data_dimensions
        assert redshift_value_meta.data_schema == expected_data_schema
        assert redshift_value_meta.columns_stats == expected_column_stats
        logging.warning("ACTUAL VALUE_PREVIEW: %s", redshift_value_meta.value_preview)
        logging.warning("EXPECTED VALUE_PREVIEW: %s", expected_value_preview)
        assert _strip_str(redshift_value_meta.value_preview) == _strip_str(
            expected_value_preview
        )
        assert redshift_value_meta.data_hash == str(hash(str(redshift_operation)))
        assert redshift_value_meta.query == COPY_FROM_S3_FILE_QUERY
        # histograms are not supported
        assert redshift_value_meta.histograms == {}
        assert redshift_value_meta.histogram_system_metrics == None


def _strip_str(value: str):
    return "\n".join([s.strip() for s in value.split("\n")])
