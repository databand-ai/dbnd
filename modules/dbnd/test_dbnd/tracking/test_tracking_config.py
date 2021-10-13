import pytest

from dbnd._core.settings.tracking_config import (
    TrackingConfig,
    ValueTrackingLevel,
    get_value_meta,
)
from targets import target as target_factory
from targets.value_meta import ValueMetaConf
from targets.values import ListValueType, ObjectValueType, StrValueType, TargetValueType


class TestTrackingConfig:
    @pytest.mark.parametrize(
        "value, value_type, target, expected_value_preview, expected_data_schema",
        [
            (10, None, None, "10", {"type": "int"}),
            (10, ObjectValueType(), None, "10", {"type": "int"}),
            (10, TargetValueType(), target_factory("/path"), "10", {"type": "int"}),
            (10, StrValueType(), None, "10", {"type": "str"}),
            (
                [10],
                ListValueType(),
                None,
                "[10]",
                {
                    "columns": [],
                    "dtypes": {},
                    "shape": (1, 0),
                    "size.bytes": 48,
                    "type": "List",
                },
            ),
            (
                [
                    {"test": "test", "num": 10, "bool": True},
                    {"test": "test_2", "num": 20, "bool": False},
                ],
                ListValueType(),
                None,
                '[{"bool":true,"num":10,"test":"test"},{"bool":false,"num":20,"test":"test_2"}]',
                {
                    "columns": ["bool", "num", "test"],
                    "dtypes": {
                        "bool": "<class 'bool'>",
                        "num": "<class 'int'>",
                        "test": "<class 'str'>",
                    },
                    "shape": (2, 3),
                    "size.bytes": 56,
                    "type": "List",
                },
            ),
        ],
    )
    def test_get_value_meta(
        self, value, value_type, target, expected_value_preview, expected_data_schema,
    ):
        tracking_config = TrackingConfig.current()
        tracking_config.value_reporting_strategy = ValueTrackingLevel.ALL

        result = get_value_meta(
            value,
            ValueMetaConf(),
            tracking_config,
            value_type=value_type,
            target=target,
        )

        assert result.value_preview == expected_value_preview
        assert result.data_schema == expected_data_schema
