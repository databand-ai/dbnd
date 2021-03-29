from functools import reduce

import pytest

from dbnd._core.settings.tracking_config import (
    ValueTrackingLevel,
    calc_meta_conf_for_value_type,
)
from targets import FileTarget
from targets.fs.mock import MockFileSystem
from targets.target_config import file
from targets.value_meta import ValueMetaConf
from targets.values import ObjectValueType
from targets.values.builtins_values import DataValueType


class LazyValueType(DataValueType):
    is_lazy_evaluated = True

    def support_fast_count(self, target):
        from targets import FileTarget

        if not isinstance(target, FileTarget):
            return False
        from targets.target_config import FileFormat

        return target.config.format == FileFormat.parquet


ALL_NONE = ValueMetaConf()
ALL_TRUE = ValueMetaConf.enabled()
ALL_FALSE = ValueMetaConf(
    log_size=False,
    log_preview=False,
    log_schema=False,
    log_stats=False,
    log_histograms=False,
)


class TestValueMetaConf(object):
    @pytest.mark.parametrize(
        "left, right, expected",
        [
            (ALL_NONE, ALL_TRUE, ALL_TRUE),
            (ALL_TRUE, ALL_NONE, ALL_TRUE),
            (
                ValueMetaConf(
                    log_schema=True, log_size=False, log_preview=True, log_stats=True
                ),
                ALL_FALSE,
                ValueMetaConf(
                    log_schema=True,
                    log_size=False,
                    log_preview=True,
                    log_stats=True,
                    log_histograms=False,
                ),
            ),
            (ALL_FALSE, ALL_TRUE, ALL_FALSE),
            (ALL_TRUE, ALL_FALSE, ALL_TRUE),
        ],
    )
    def test_merging_2(self, left, right, expected):
        assert left.merge_if_none(right) == expected

    @pytest.mark.parametrize(
        "meta_conf_list, expected",
        [
            ([ALL_NONE, ALL_TRUE, ALL_FALSE], ALL_TRUE),
            ([ALL_NONE, ALL_NONE, ALL_FALSE], ALL_FALSE),
            (
                [
                    ALL_NONE,
                    ValueMetaConf(
                        log_preview=True,
                        log_schema=True,
                        log_size=True,
                        log_stats=False,
                    ),
                    ALL_FALSE,
                ],
                ValueMetaConf(
                    log_preview=True,
                    log_schema=True,
                    log_size=True,
                    log_stats=False,
                    log_histograms=False,
                ),
            ),
        ],
    )
    def test_summing(self, meta_conf_list, expected):
        assert reduce(lambda x, y: x.merge_if_none(y), meta_conf_list) == expected


@pytest.mark.parametrize(
    "level, value_type, target, expected",
    [
        # ValueTrackingLevel.NONE
        (
            ValueTrackingLevel.NONE,
            ObjectValueType(),
            None,
            ValueMetaConf(
                log_preview=False,
                log_histograms=False,
                log_stats=False,
                log_size=False,
            ),
        ),
        (
            ValueTrackingLevel.NONE,
            LazyValueType(),
            None,
            ValueMetaConf(
                log_preview=False,
                log_histograms=False,
                log_stats=False,
                log_size=False,
            ),
        ),
        (
            ValueTrackingLevel.NONE,
            LazyValueType(),
            FileTarget("mock_path", MockFileSystem()),
            ValueMetaConf(
                log_preview=False,
                log_histograms=False,
                log_stats=False,
                log_size=False,
            ),
        ),
        (
            ValueTrackingLevel.NONE,
            LazyValueType(),
            FileTarget("mock_path", MockFileSystem(), config=file.parquet),
            ValueMetaConf(
                log_preview=False,
                log_histograms=False,
                log_stats=False,
                log_size=False,
            ),
        ),
        # ValueTrackingLevel.SMART
        (ValueTrackingLevel.SMART, ObjectValueType(), None, ValueMetaConf(),),
        (
            ValueTrackingLevel.SMART,
            LazyValueType(),
            None,
            ValueMetaConf(
                log_preview=False, log_histograms=False, log_stats=False, log_size=None,
            ),
        ),
        (
            ValueTrackingLevel.SMART,
            LazyValueType(),
            FileTarget("mock_path", MockFileSystem(), config=file),
            ValueMetaConf(
                log_preview=False,
                log_histograms=False,
                log_stats=False,
                log_size=False,
            ),
        ),
        (
            ValueTrackingLevel.SMART,
            LazyValueType(),
            FileTarget("mock_path", MockFileSystem(), config=file.parquet),
            ValueMetaConf(
                log_preview=False, log_histograms=False, log_stats=False, log_size=True,
            ),
        ),
        (ValueTrackingLevel.ALL, ObjectValueType(), None, ValueMetaConf(),),
        (ValueTrackingLevel.ALL, LazyValueType(), None, ValueMetaConf(),),
        (
            ValueTrackingLevel.ALL,
            LazyValueType(),
            FileTarget("mock_path", MockFileSystem()),
            ValueMetaConf(),
        ),
        (
            ValueTrackingLevel.ALL,
            LazyValueType(),
            FileTarget("mock_path", MockFileSystem(), config=file.parquet),
            ValueMetaConf(),
        ),
    ],
)
def test_value_tracking_level(level, value_type, target, expected):
    assert expected == calc_meta_conf_for_value_type(level, value_type, target)
