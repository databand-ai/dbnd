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

    @pytest.mark.parametrize(
        "value, value_type, target, expected_value_preview, expected_data_schema",
        [
            (
                [
                    {"test": "test", "num": 10, "bool": True},
                    {"test": "test_2", "num": 20, "bool": False},
                ],
                ListValueType(),
                None,
                '[{"test": "test", "num": 10, "bool": true}, {"test": "test_2", "num": 20, '
                '"bool": false}]',
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
    def test_get_value_meta_list_of_flat_dict(
        self, value, value_type, target, expected_value_preview, expected_data_schema
    ):
        self.test_get_value_meta(
            value, value_type, target, expected_value_preview, expected_data_schema
        )

    @pytest.mark.parametrize(
        "value, value_type, target, expected_value_preview, expected_data_schema",
        [
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
                '[{"test": "test", "num": 10, "bool": true, "test_object": '
                '{"test_object_name": "some_name", "test_id": 12345, "inner": {"foo": '
                '"bar"}}}, {"test": "test_2", "num": 20, "bool": false, "test_object": '
                '{"test_object_name": "some_other_name", "test_id": 56789, "inner": {"foo": '
                '"bar_2"}}}]',
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
        ],
    )
    def test_get_value_meta_list_of_nested_dict(
        self, value, value_type, target, expected_value_preview, expected_data_schema
    ):
        self.test_get_value_meta(
            value, value_type, target, expected_value_preview, expected_data_schema
        )

    @pytest.mark.parametrize(
        "value, value_type, target, expected_value_preview, expected_data_schema",
        [
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
                '[{"test": "test", "num": 10, "bool": true, "test_objects": '
                '[{"test_object_name": "some_name", "test_id": 12345}]}, {"test": "test_2", '
                '"num": 20, "bool": false, "test_objects": [{"test_object_name": '
                '"some_other_name", "test_id": 56789}]}]',
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
        ],
    )
    def test_get_value_meta_list_of_dict_with_list_of_flat_dict(
        self, value, value_type, target, expected_value_preview, expected_data_schema
    ):
        self.test_get_value_meta(
            value, value_type, target, expected_value_preview, expected_data_schema
        )

    @pytest.mark.parametrize(
        "value, value_type, target, expected_value_preview, expected_data_schema",
        [
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
                '[{"test": "test", "num": 10, "bool": true, "test_object": '
                '{"test_object_name": "some_other_name", "test_id": 56789, "inner": {"foo": '
                '"bar_3"}}, "test_objects": [{"test_object_name": "some_name", "test_id": '
                '12345, "inner": {"foo": "bar"}}, {"test_object_name": "some_name_1", '
                '"test_id": 54321, "inner": {"foo": "bar_2"}}]}, {"test": "test_2", "num": '
                '20, "bool": false, "test_object": {"test_object_name": "some_name", '
                '"test_id": 11111, "inner": {"foo": "bar_6"}}, "test_objects": '
                '[{"test_object_name": "some_other_name", "test_id": 56789, "inner": {"foo": '
                '"bar_3"}}, {"test_object_name": "some_other_name", "test_id": 98765, '
                '"inner": {"foo": "bar_4"}}]}]',
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
        ],
    )
    def test_get_value_meta_list_of_dict_with_list_of_nested_dicts(
        self, value, value_type, target, expected_value_preview, expected_data_schema
    ):
        self.test_get_value_meta(
            value, value_type, target, expected_value_preview, expected_data_schema
        )

    @pytest.mark.parametrize(
        "value, value_type, target, expected_value_preview, expected_data_schema",
        [
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
                '[{"test": "test", "num": 10, "bool": true, "test_object": '
                '{"test_object_name": "some_other_name", "test_id": 56789, "inner": {"foo": '
                '"bar_3"}}, "test_objects": [{"test_object_name": "some_name", "test_id": '
                '12345, "inner": {"foo": "bar"}}, {"test_object_name": "some_name_1", '
                '"test_id": 54321, "inner": {"foo": "bar_2"}}]}, {"test_1": "test_2", "num": '
                '20, "bool": false, "test_object": {"test_object_name": "some_name", '
                '"test_id": 11111, "inner": {"foo": "bar_6", "bar": "foo"}}, "test_objects": '
                '[{"test_object_name": "some_other_name", "test_id": 56789, "inner": {"foo": '
                '"bar_3"}}, {"test_object_name": "some_other_name", "test_id": 98765, '
                '"inner": {"foo": "bar_4"}}]}]',
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
    def test_get_value_meta_list_of_dict_with_list_of_nested_dicts_with_different_keys(
        self, value, value_type, target, expected_value_preview, expected_data_schema
    ):
        self.test_get_value_meta(
            value, value_type, target, expected_value_preview, expected_data_schema
        )

    @pytest.mark.parametrize(
        "value, value_type, target, expected_value_preview",
        [
            (
                [
                    {"a": "a", "b": "b"},
                    {"a": "c", "b": "d"},
                    {"a": "e", "b": "f"},
                    {"a": "g", "b": "h"},
                    {"a": "i", "b": "j"},
                    {"a": "k", "b": "l"},
                    {"a": "m", "b": "n"},
                    {"a": "o", "b": "p"},
                    {"a": "q", "b": "r"},
                    {"a": "s", "b": "t"},
                    {"a": "u", "b": "v"},
                    {"a": "w", "b": "x"},
                    {"a": "y", "b": "z"},
                ],
                ListValueType(),
                None,
                '[{"a": "a", "b": "b"}, {"a": "c", "b": "d"}, {"a": "e", "b": "f"}, {"a": '
                '"g", "b": "h"}, {"a": "i", "b": "j"}, {"a": "k", "b": "l"}, {"a": "m", "b": '
                '"n"}, {"a": "o", "b": "p"}, {"a": "q", "b": "r"}, {"a": "s", "b": "t"}]',
            ),
        ],
    )
    def test_get_value_meta_preview_size_default_max_elements(
        self, value, value_type, target, expected_value_preview,
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

    @pytest.mark.parametrize(
        "value, value_type, target, expected_value_preview",
        [
            (
                [
                    {"a": "a", "b": "b"},
                    {"a": "c", "b": "d"},
                    {"a": "e", "b": "f"},
                    {"a": "g", "b": "h"},
                    {"a": "i", "b": "j"},
                    {"a": "k", "b": "l"},
                    {"a": "m", "b": "n"},
                    {"a": "o", "b": "p"},
                    {"a": "q", "b": "r"},
                    {"a": "s", "b": "t"},
                    {"a": "u", "b": "v"},
                    {"a": "w", "b": "x"},
                    {"a": "y", "b": "z"},
                ],
                ListValueType(),
                None,
                '[{"a": "a", "b": "b"}, {"a": "c", "b": "d"}, {"a": "e", "b": "f"}, {"a": '
                '"g", "b": "h"}, {"a": "i", "b": "j"}]',
            ),
        ],
    )
    def test_get_value_meta_preview_size_config_max_elements(
        self, value, value_type, target, expected_value_preview,
    ):
        tracking_config = TrackingConfig.current()
        tracking_config.value_reporting_strategy = ValueTrackingLevel.ALL

        result = get_value_meta(
            value,
            ValueMetaConf(log_preview_size=5),
            tracking_config,
            value_type=value_type,
            target=target,
        )

        assert result.value_preview == expected_value_preview

    @pytest.mark.parametrize(
        "value, value_type, target, expected_value_preview",
        [([{"a": "a", "b": "b"},], ListValueType(), None, '[{"a": "a", "b": "b"}]',),],
    )
    def test_get_value_meta_preview_small_size(
        self, value, value_type, target, expected_value_preview,
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
