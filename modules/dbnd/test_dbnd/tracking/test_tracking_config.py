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
            # list of flat dict
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
            #  list of dict with nested dicts
            (
                [
                    {
                        "test": "test",
                        "num": 10,
                        "bool": True,
                        "test_object": {
                            "test_object_name": "some_name",
                            "test_id": 12345,
                            "inner": {"foo": "bar"},
                        },
                    },
                    {
                        "test": "test_2",
                        "num": 20,
                        "bool": False,
                        "test_object": {
                            "test_object_name": "some_other_name",
                            "test_id": 56789,
                            "inner": {"foo": "bar_2"},
                        },
                    },
                ],
                ListValueType(),
                None,
                '[{"bool":true,"num":10,"test":"test","test_object":{"inner":{"foo":"bar"},"test_id":12345,"test_object_name":"some_name"}},{"bool":false,"num":20,"test":"test_2","test_object":{"inner":{"foo":"bar_2"},"test_id":56789,"test_object_name":"some_other_name"}}]',
                {
                    "columns": [
                        "bool",
                        "num",
                        "test",
                        "test_object",
                        "test_object.inner.foo",
                        "test_object.test_id",
                        "test_object.test_object_name",
                    ],
                    "dtypes": {
                        "bool": "<class 'bool'>",
                        "num": "<class 'int'>",
                        "test": "<class 'str'>",
                        "test_object": "<class 'dict'>",
                        "test_object.inner.foo": "<class 'str'>",
                        "test_object.test_id": "<class 'int'>",
                        "test_object.test_object_name": "<class 'str'>",
                    },
                    "shape": (2, 7),
                    "size.bytes": 56,
                    "type": "List",
                },
            ),
            #  list of dict with arrays of flat dicts
            (
                [
                    {
                        "test": "test",
                        "num": 10,
                        "bool": True,
                        "test_objects": [
                            {"test_object_name": "some_name", "test_id": 12345}
                        ],
                    },
                    {
                        "test": "test_2",
                        "num": 20,
                        "bool": False,
                        "test_objects": [
                            {"test_object_name": "some_other_name", "test_id": 56789}
                        ],
                    },
                ],
                ListValueType(),
                None,
                '[{"bool":true,"num":10,"test":"test","test_objects":[{"test_id":12345,"test_object_name":"some_name"}]},{"bool":false,"num":20,"test":"test_2","test_objects":[{"test_id":56789,"test_object_name":"some_other_name"}]}]',
                {
                    "columns": ["bool", "num", "test", "test_objects"],
                    "dtypes": {
                        "bool": "<class 'bool'>",
                        "num": "<class 'int'>",
                        "test": "<class 'str'>",
                        "test_objects": "<class 'list'>",
                    },
                    "shape": (2, 4),
                    "size.bytes": 56,
                    "type": "List",
                },
            ),
            #  list of dict with arrays of dicts and nested dicts
            (
                [
                    {
                        "test": "test",
                        "num": 10,
                        "bool": True,
                        "test_object": {
                            "test_object_name": "some_other_name",
                            "test_id": 56789,
                            "inner": {"foo": "bar_3"},
                        },
                        "test_objects": [
                            {
                                "test_object_name": "some_name",
                                "test_id": 12345,
                                "inner": {"foo": "bar"},
                            },
                            {
                                "test_object_name": "some_name_1",
                                "test_id": 54321,
                                "inner": {"foo": "bar_2"},
                            },
                        ],
                    },
                    {
                        "test": "test_2",
                        "num": 20,
                        "bool": False,
                        "test_object": {
                            "test_object_name": "some_name",
                            "test_id": 11111,
                            "inner": {"foo": "bar_6"},
                        },
                        "test_objects": [
                            {
                                "test_object_name": "some_other_name",
                                "test_id": 56789,
                                "inner": {"foo": "bar_3"},
                            },
                            {
                                "test_object_name": "some_other_name",
                                "test_id": 98765,
                                "inner": {"foo": "bar_4"},
                            },
                        ],
                    },
                ],
                ListValueType(),
                None,
                '[{"bool":true,"num":10,"test":"test","test_object":{"inner":{"foo":"bar_3"},"test_id":56789,"test_object_name":"some_other_name"},"test_objects":[{"inner":{"foo":"bar"},"test_id":12345,"test_object_name":"some_name"},{"inner":{"foo":"bar_2"},"test_id":54321,"test_object_name":"some_name_1"}]},{"bool":false,"num":20,"test":"test_2","test_object":{"inner":{"foo":"bar_6"},"test_id":11111,"test_object_name":"some_name"},"test_objects":[{"inner":{"foo":"bar_3"},"test_id":56789,"test_object_name":"some_other_name"},{"inner":{"foo":"bar_4"},"test_id":98765,"test_object_name":"some_other_name"}]}]',
                {
                    "columns": [
                        "bool",
                        "num",
                        "test",
                        "test_object",
                        "test_object.inner.foo",
                        "test_object.test_id",
                        "test_object.test_object_name",
                        "test_objects",
                    ],
                    "dtypes": {
                        "bool": "<class 'bool'>",
                        "num": "<class 'int'>",
                        "test": "<class 'str'>",
                        "test_object": "<class 'dict'>",
                        "test_object.inner.foo": "<class 'str'>",
                        "test_object.test_id": "<class 'int'>",
                        "test_object.test_object_name": "<class 'str'>",
                        "test_objects": "<class 'list'>",
                    },
                    "shape": (2, 8),
                    "size.bytes": 56,
                    "type": "List",
                },
            ),
            #  list of dict with arrays of dicts and nested dicts with different keys
            (
                [
                    {
                        "test": "test",
                        "num": 10,
                        "bool": True,
                        "test_object": {
                            "test_object_name": "some_other_name",
                            "test_id": 56789,
                            "inner": {"foo": "bar_3"},
                        },
                        "test_objects": [
                            {
                                "test_object_name": "some_name",
                                "test_id": 12345,
                                "inner": {"foo": "bar"},
                            },
                            {
                                "test_object_name": "some_name_1",
                                "test_id": 54321,
                                "inner": {"foo": "bar_2"},
                            },
                        ],
                    },
                    {
                        "test_1": "test_2",
                        "num": 20,
                        "bool": False,
                        "test_object": {
                            "test_object_name": "some_name",
                            "test_id": 11111,
                            "inner": {"foo": "bar_6", "bar": "foo"},
                        },
                        "test_objects": [
                            {
                                "test_object_name": "some_other_name",
                                "test_id": 56789,
                                "inner": {"foo": "bar_3"},
                            },
                            {
                                "test_object_name": "some_other_name",
                                "test_id": 98765,
                                "inner": {"foo": "bar_4"},
                            },
                        ],
                    },
                ],
                ListValueType(),
                None,
                '[{"bool":true,"num":10,"test":"test","test_object":{"inner":{"foo":"bar_3"},"test_id":56789,"test_object_name":"some_other_name"},"test_objects":[{"inner":{"foo":"bar"},"test_id":12345,"test_object_name":"some_name"},{"inner":{"foo":"bar_2"},"test_id":54321,"test_object_name":"some_name_1"}]},{"bool":false,"num":20,"test_1":"test_2","test_object":{"inner":{"bar":"foo","foo":"bar_6"},"test_id":11111,"test_object_name":"some_name"},"test_objects":[{"inner":{"foo":"bar_3"},"test_id":56789,"test_object_name":"some_other_name"},{"inner":{"foo":"bar_4"},"test_id":98765,"test_object_name":"some_other_name"}]}]',
                {
                    "columns": [
                        "bool",
                        "num",
                        "test",
                        "test_1",
                        "test_object",
                        "test_object.inner.bar",
                        "test_object.inner.foo",
                        "test_object.test_id",
                        "test_object.test_object_name",
                        "test_objects",
                    ],
                    "dtypes": {
                        "bool": "<class 'bool'>",
                        "num": "<class 'int'>",
                        "test": "<class 'str'>",
                        "test_1": "<class 'str'>",
                        "test_object": "<class 'dict'>",
                        "test_object.inner.bar": "<class 'str'>",
                        "test_object.inner.foo": "<class 'str'>",
                        "test_object.test_id": "<class 'int'>",
                        "test_object.test_object_name": "<class 'str'>",
                        "test_objects": "<class 'list'>",
                    },
                    "shape": (2, 10),
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
