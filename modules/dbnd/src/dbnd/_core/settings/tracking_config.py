# © Copyright Databand.ai, an IBM Company 2022

import enum
import logging

from typing import Any, Dict, Optional

import attr

from dbnd._core.parameter import PARAMETER_FACTORY as parameter
from dbnd._core.task import Config
from targets import Target
from targets.value_meta import _DEFAULT_VALUE_PREVIEW_MAX_LEN, ValueMeta, ValueMetaConf
from targets.values import (
    ObjectValueType,
    TargetValueType,
    ValueType,
    get_value_type_of_obj,
)


logger = logging.getLogger()


class ValueTrackingLevel(enum.Enum):
    """
    Multiple strategies with different limitations on potentially expensive calculation for value_meta
    """

    NONE = 1
    SMART = 2
    ALL = 3


class TrackingConfig(Config):
    _conf__task_family = "tracking"

    project = parameter(
        default=None,
        description="Set the project to which the run should be assigned. "
        "If this is not set, the default project is used. "
        "The tracking server will select a project with `is_default == True`.",
    )[str]

    job = parameter(
        default=None,
        description="Set the job name to which the run should be assigned. "
        "If this is not set, the job name is calcualted from the runtime "
        "(airflow dag_id, script name, etc) ",
    )[str]

    databand_external_url = parameter(
        default=None,
        description="Set tracker URL to be used for tracking from external systems.",
    )[str]

    log_value_size = parameter(
        default=True,
        description="Should this calculate and log the value's size? "
        "This can cause a full scan on non-indexable distributed memory objects.",
    )[bool]

    log_value_schema = parameter(
        default=True, description="Should this calculate and log the value's schema?"
    )[bool]

    log_value_stats = parameter(
        default=True,
        description="Should this calculate and log the value's stats? "
        "This is expensive to calculate, so it might be better to use log_stats on the parameter level.",
    )[bool]

    log_value_preview = parameter(
        default=True,
        description="Should this calculate and log the value's preview? This can be expensive to calculate on Spark.",
    )[bool]

    log_value_preview_max_len = parameter(
        description="Set the max size of the value's preview to be saved at the DB. The max value of this parameter "
        "is 50000"
    ).value(_DEFAULT_VALUE_PREVIEW_MAX_LEN)

    log_value_meta = parameter(
        default=True, description="Should this calculate and log the value's meta?"
    )[bool]

    log_histograms = parameter(
        default=True,
        description="Enable calculation and tracking of histograms. This can be expensive.",
    )[bool]

    value_reporting_strategy = parameter(
        default=ValueTrackingLevel.SMART,
        description="Set the strategy used for the reporting of values. There are multiple strategies, "
        "each with different limitations on potentially expensive calculations for value_meta.\n"
        "`ALL` means there are no limitations.\n"
        "`SMART` means there are restrictions on lazy evaluation types.\n"
        "`NONE`, which is the default value, limits everything.",
    ).enum(ValueTrackingLevel)

    auto_disable_slow_size = parameter(
        default=True,
        description="Enable automatically disabling slow previews for Spark DataFrame with text formats.",
    )[bool]

    track_source_code = parameter(
        default=False,
        description="Enable tracking of function, module and file source code.",
    )[bool]

    airflow_operator_handlers = parameter(
        default={},
        description="Control which of the Airflow Operator's fields would be flattened when tracked.",
    )[Dict[str, str]]

    flatten_operator_fields = parameter(
        default={},
        description="Control which of the Airflow Operator's fields would be flattened when tracked.",
    )[Dict[str, str]]

    def get_value_meta_conf(self, meta_conf, value_type, target=None):
        # type: (ValueMetaConf, ValueType, Optional[Target]) -> ValueMetaConf
        meta_conf_by_type = calc_meta_conf_for_value_type(
            self.value_reporting_strategy, value_type, target
        )
        # translating TrackingConfig to meta_conf
        meta_conf_by_config = self._build_meta_conf()
        return meta_conf.merge_if_none(meta_conf_by_type).merge_if_none(
            meta_conf_by_config
        )

    def _build_meta_conf(self):
        # type: () -> ValueMetaConf
        """
        Translate this configuration into value meta conf
        WE EXPECT IT TO HAVE ALL THE INNER VALUES SET WITHOUT NONES
        """
        return ValueMetaConf(
            log_schema=self.log_value_schema,
            log_size=self.log_value_size,
            log_preview_size=self.log_value_preview_max_len,
            log_preview=self.log_value_preview,
            log_stats=self.log_value_stats,
            log_histograms=self.log_histograms,
        )


def _is_default_value_type(value_type):
    return value_type is None or isinstance(value_type, ObjectValueType)


def get_value_meta(value, meta_conf, tracking_config, value_type=None, target=None):
    # type: ( Any, ValueMetaConf, TrackingConfig, Optional[ValueType], Optional[Target]) -> Optional[ValueMeta]
    """
    Build the value meta for tracking logging.
    Using the given meta config, the value, and tracking_config to calculate the required value meta.

    @param value: the value to calc value meta for
    @param meta_conf: a given meta_config by a user
    @param tracking_config: TrackingConfig to calc the wanted meta conf
    @param value_type: optional value_type, if its known.
    @param target: knowledge about the target which contains the value - this can effect the cost of the calculation
    @return: Calculated value meta
    """

    if value is None:
        return None

    # required for calculating the relevant configuration and to build value_meta
    if _is_default_value_type(value_type) or isinstance(value_type, TargetValueType):
        # we calculate the actual value_type even if the given value is the default value
        # so we can be sure that we can report it the right way
        # also Targets are futures types and now would can log their actual value
        value_type = get_value_type_of_obj(value, default_value_type=ObjectValueType())

    meta_conf = tracking_config.get_value_meta_conf(meta_conf, value_type, target)
    return value_type.get_value_meta(value, meta_conf=meta_conf)


def calc_meta_conf_for_value_type(tracking_level, value_type, target=None):
    # type: (ValueTrackingLevel, ValueType, Optional[Target]) -> ValueMetaConf
    """
    Calculating the right value log config base on the value type in order control the tracking of
    lazy evaluated types like spark dataframes

    IMPORTANT - The result is ValueMetaConf with restrictions only! this should be merged into a full ValueMetaConf.
    """

    if tracking_level == ValueTrackingLevel.ALL:
        # no restrictions
        return ValueMetaConf()

    if tracking_level == ValueTrackingLevel.SMART:

        result = ValueMetaConf()
        if value_type.is_lazy_evaluated:
            # restrict only for lazy evaluate values
            result = ValueMetaConf.disabled_expensive()

        elif target is not None and not value_type.support_fast_count(target):
            # we don't set it to True cause there might
            # be different configuration that will want it to be False
            result = attr.evolve(result, log_size=False)

        return result

    if tracking_level == ValueTrackingLevel.NONE:
        # restrict any
        return ValueMetaConf.disabled_expensive()
