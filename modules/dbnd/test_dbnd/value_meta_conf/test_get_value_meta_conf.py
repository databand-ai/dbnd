from typing import Callable

import pytest

from dbnd import LogDataRequest, config
from dbnd._core.configuration.config_store import _ConfigMergeSettings
from dbnd._core.settings import TrackingConfig
from targets.value_meta import _DEFAULT_VALUE_PREVIEW_MAX_LEN, ValueMetaConf


def tracking_config_empty():
    # Enforce "tracking" config section so that changes in config files won't affect tests
    with config(
        {"tracking": {}},
        source="dbnd_test_context",
        merge_settings=_ConfigMergeSettings(replace_section=True),
    ):
        return TrackingConfig()


def tracking_config_force_true():
    # Enforce "tracking" config section so that changes in config files won't affect tests
    config.set("tracking", "log_value_stats", True, override=True)
    config.set("tracking", "log_histograms", True, override=True)
    return TrackingConfig()


def tracking_config_force_false():
    # Enforce "tracking" config section so that changes in config files won't affect tests
    config.set("tracking", "log_value_stats", False, override=True)
    config.set("tracking", "log_histograms", False, override=True)
    return TrackingConfig()


class TestTrackingConfigEmptyGetValueMetaConf(object):
    @pytest.mark.parametrize(
        "tracking_config, param_log_stats, config_log_stats, expected_log_stats",
        [
            # Explicit config value
            (tracking_config_empty, None, True, True),
            (tracking_config_empty, None, False, False),
            (tracking_config_empty, False, True, False),
            (tracking_config_empty, False, False, False),
            (tracking_config_empty, True, True, True),
            (tracking_config_empty, True, False, True),
            # Default config value
            (tracking_config_empty, None, None, False),
            (tracking_config_empty, False, None, False),
            (tracking_config_empty, True, None, True),
            # Explicit config value
            (tracking_config_force_true, None, True, True),
            (tracking_config_force_true, None, False, False),
            (tracking_config_force_true, False, True, False),
            (tracking_config_force_true, False, False, False),
            (tracking_config_force_true, True, True, True),
            (tracking_config_force_true, True, False, True),
            # Default config value
            (tracking_config_force_true, None, None, True),
            (tracking_config_force_true, False, None, False),
            (tracking_config_force_true, True, None, True),
            # Explicit config value
            (tracking_config_force_false, None, True, True),
            (tracking_config_force_false, None, False, False),
            (tracking_config_force_false, False, True, False),
            (tracking_config_force_false, False, False, False),
            (tracking_config_force_false, True, True, True),
            (tracking_config_force_false, True, False, True),
            # Default config value
            (tracking_config_force_false, None, None, False),
            (tracking_config_force_false, False, None, False),
            (tracking_config_force_false, True, None, True),
        ],
    )
    def test_log_stats(
        self, tracking_config, param_log_stats, config_log_stats, expected_log_stats,
    ):  # type: (Callable[[], TrackingConfig], bool, bool, bool) -> None
        # Arrange
        tc = tracking_config()
        param_mc = ValueMetaConf(log_stats=param_log_stats)
        if config_log_stats is not None:
            tc.log_value_stats = config_log_stats
        expected_log_stats = LogDataRequest(
            include_all_boolean=expected_log_stats,
            include_all_numeric=expected_log_stats,
            include_all_string=expected_log_stats,
        )

        # Act
        actual_value_meta_conf = tc.get_value_meta_conf(param_mc)

        # Assert
        assert actual_value_meta_conf.log_stats == expected_log_stats

    @pytest.mark.parametrize(
        "tracking_config, param_log_histograms, config_log_histograms, expected_log_histograms",
        [
            # Explicit config value
            (tracking_config_empty, None, True, True),
            (tracking_config_empty, None, False, False),
            (tracking_config_empty, False, True, False),
            (tracking_config_empty, False, False, False),
            (tracking_config_empty, True, True, True),
            (tracking_config_empty, True, False, True),
            # Default config value
            (tracking_config_empty, None, None, False),
            (tracking_config_empty, False, None, False),
            (tracking_config_empty, True, None, True),
            # Explicit config value
            (tracking_config_force_true, None, True, True),
            (tracking_config_force_true, None, False, False),
            (tracking_config_force_true, False, True, False),
            (tracking_config_force_true, False, False, False),
            (tracking_config_force_true, True, True, True),
            (tracking_config_force_true, True, False, True),
            # Default config value
            (tracking_config_force_true, None, None, True),
            (tracking_config_force_true, False, None, False),
            (tracking_config_force_true, True, None, True),
            # Explicit config value
            (tracking_config_force_false, None, True, True),
            (tracking_config_force_false, None, False, False),
            (tracking_config_force_false, False, True, False),
            (tracking_config_force_false, False, False, False),
            (tracking_config_force_false, True, True, True),
            (tracking_config_force_false, True, False, True),
            # Default config value
            (tracking_config_force_false, None, None, False),
            (tracking_config_force_false, False, None, False),
            (tracking_config_force_false, True, None, True),
        ],
    )
    def test_log_histograms(
        self,
        tracking_config,  # type: Callable[[], TrackingConfig]
        param_log_histograms,  # type: bool
        config_log_histograms,  # type: bool
        expected_log_histograms,  # type: bool
    ):
        # Arrange
        tc = tracking_config()
        param_mc = ValueMetaConf(log_histograms=param_log_histograms)
        if config_log_histograms is not None:
            tc.log_histograms = config_log_histograms
        expected_log_histograms = LogDataRequest(
            include_all_boolean=expected_log_histograms,
            include_all_numeric=expected_log_histograms,
            include_all_string=expected_log_histograms,
        )

        # Act
        actual_value_meta_conf = tc.get_value_meta_conf(param_mc)

        # Assert
        assert actual_value_meta_conf.log_histograms == expected_log_histograms

    @pytest.mark.parametrize(
        "tracking_config, param_log_preview, config_log_preview, expected_log_preview",
        [
            # Explicit config value
            (tracking_config_empty, None, True, True),
            (tracking_config_empty, None, False, False),
            (tracking_config_empty, False, True, False),
            (tracking_config_empty, False, False, False),
            (tracking_config_empty, True, True, True),
            (tracking_config_empty, True, False, True),
            # Default config value
            (tracking_config_empty, None, None, True),
            (tracking_config_empty, False, None, False),
            (tracking_config_empty, True, None, True),
        ],
    )
    def test_log_preview(
        self,
        tracking_config,  # type: Callable[[], TrackingConfig]
        param_log_preview,  # type: bool
        config_log_preview,  # type: bool
        expected_log_preview,  # type: bool
    ):
        # Arrange
        tc = tracking_config()
        param_mc = ValueMetaConf(log_preview=param_log_preview)
        if config_log_preview is not None:
            tc.log_value_preview = config_log_preview

        # Act
        actual_value_meta_conf = tc.get_value_meta_conf(param_mc)

        # Assert
        assert actual_value_meta_conf.log_preview == expected_log_preview

    @pytest.mark.parametrize(
        "tracking_config, param_log_preview_size, config_log_preview_size, expected_log_preview_size",
        [
            (tracking_config_empty, None, None, _DEFAULT_VALUE_PREVIEW_MAX_LEN),
            (tracking_config_empty, None, 42, 42),
            (tracking_config_empty, 42, None, 42),
            (tracking_config_empty, 42, 314, 42),
        ],
    )
    def test_log_preview_size(
        self,
        tracking_config,
        param_log_preview_size,
        config_log_preview_size,
        expected_log_preview_size,
    ):  # type: (Callable[[], TrackingConfig], int, int, int) -> None
        # Arrange
        tc = tracking_config()
        param_mc = ValueMetaConf(log_preview_size=param_log_preview_size)
        if config_log_preview_size is not None:
            tc.log_value_preview_max_len = config_log_preview_size

        # Act
        actual_value_meta_conf = tc.get_value_meta_conf(param_mc)

        # Assert
        assert actual_value_meta_conf.log_preview_size == expected_log_preview_size

    @pytest.mark.parametrize(
        "tracking_config, param_log_schema, config_log_schema, expected_log_schema",
        [
            # Explicit config value
            (tracking_config_empty, None, True, True),
            (tracking_config_empty, None, False, False),
            (tracking_config_empty, False, True, False),
            (tracking_config_empty, False, False, False),
            (tracking_config_empty, True, True, True),
            (tracking_config_empty, True, False, True),
            # Default config value
            (tracking_config_empty, None, None, True),
            (tracking_config_empty, False, None, False),
            (tracking_config_empty, True, None, True),
        ],
    )
    def test_log_schema(
        self, tracking_config, param_log_schema, config_log_schema, expected_log_schema,
    ):  # type: (Callable[[], TrackingConfig], bool, bool, bool) -> None
        # Arrange
        tracking_config = tracking_config()
        param_mc = ValueMetaConf(log_schema=param_log_schema)
        if config_log_schema is not None:
            tracking_config.log_value_schema = config_log_schema

        # Act
        actual_value_meta_conf = tracking_config.get_value_meta_conf(param_mc)

        # Assert
        assert actual_value_meta_conf.log_schema == expected_log_schema
