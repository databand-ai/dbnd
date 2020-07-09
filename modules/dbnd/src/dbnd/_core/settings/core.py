import logging

from typing import Dict, List

from dbnd._core.constants import CloudType
from dbnd._core.current import get_settings
from dbnd._core.errors import friendly_error
from dbnd._core.parameter import PARAMETER_FACTORY as parameter
from dbnd._core.plugin.dbnd_plugins import assert_web_enabled
from dbnd._core.task import Config
from targets import Target
from targets.value_meta import _DEFAULT_VALUE_PREVIEW_MAX_LEN, ValueMetaConf
from targets.values import ValueType


logger = logging.getLogger()


class DatabandSystemConfig(Config):
    """Databand's command line arguments (see `dbnd run --help`)"""

    _conf__task_family = "databand"

    verbose = parameter(description="Make logging output more verbose").value(False)

    describe = parameter.help("Describe current run").value(False)

    module = parameter(
        default=None, description="Auto load this module before resolving user classes"
    )[str]
    env = parameter(
        default=CloudType.local,
        description="task environment: local/aws/aws_prod/gcp/prod",
    )[str]

    conf_file = parameter(default=None, description="List of files to read from")[
        List[str]
    ]
    # keep it last one, parameter keyword is used for parameters generation
    # all following params will be dict
    conf = parameter(
        default=None,
        description="JSON string/key=value that gets into the Task attribute",
    )[Dict[str, str]]

    project_name = parameter(
        default="databand_project", description="Name of this databand project"
    )[str]


class CoreConfig(Config):
    """Databand's core functionality behaviour"""

    _conf__task_family = "core"

    dags_subdir = parameter.system(
        default="",
        description="File location or directory from which to look for the dag",
    )

    environments = parameter(description="List of enabled environments")[List[str]]

    databand_url = parameter(
        default=None,
        description="Tracker URL to be used for creating links in console logs",
    )[str]

    databand_external_url = parameter(
        default=None,
        description="Tracker URL to be used for tracking from external systems",
    )[str]

    # Backward compatibility
    tracker_url = parameter(
        default=None,
        description="OLD: Tracker URL to be used for creating links in console logs",
    )[str]

    tracker_version = parameter[str]

    user_configs = parameter(
        empty_default=True,
        description="Contains the config for creating tasks from user code",
    )[List[str]]

    # user_pre_init = defined at Databand System config, dbnd_on_pre_init_context
    user_init = parameter(
        default=None,
        description="Runs in every DBND process with System configuration in place (dbnd_post_enter_context)",
    )[object]
    user_driver_init = parameter(
        default=None,
        description="Runs in driver after config initialization (dbnd_on_new_context)",
    )[object]

    user_code_on_fork = parameter(
        default=None, description="Runs in sub process (parallel/kubernets/external)"
    )[object]

    pickle_handler = parameter(
        default=None,
        description="Defines a python pickle handler to be used to pickle the "
        "run's data",
    )[str]

    tracker = parameter(
        default=["file", "console", "api"], description="Tracking Stores to be used"
    )[List[str]]
    tracker_raise_on_error = parameter(
        default=True, description="Raise error when failed to track data"
    )[bool]
    tracker_api = parameter(default="web", description="Tracking Stores to be used")[
        str
    ]

    always_save_pipeline = parameter(
        description="Boolean for always saving pipeline to pickle"
    ).value(False)
    disable_save_pipeline = parameter(
        description="Boolean for disabling pipeline pickling"
    ).value(False)

    recheck_circle_dependencies = parameter(
        description="Re check circle dependencies on every task creation,"
        " use it if you need to find of circle in your graph "
    ).value(False)

    hide_system_pipelines = parameter(
        description="Hides the scheduled job launcher and driver submit pipelines at the API level to prevent clutter",
        default=True,
    )

    fix_env_on_osx = parameter(
        description="add no_proxy=* to env vars, fixing issues with multiprocessing on osx"
    )[bool]

    plugins = parameter(
        description="plugins to load on databand context creation", default=None
    )[str]

    def _validate(self):
        if self.databand_url and self.databand_url.endswith("/"):
            logger.warning(
                "Please fix your core.databand_url value, "
                "it should not contain '/' at the end, auto-fix has been applied."
            )
            self.databand_url = self.databand_url[:-1]
        if not self.databand_external_url:
            self.databand_external_url = self.databand_url


class DynamicTaskConfig(Config):
    """
    Configuration section for dynamically generated tasks.
    """

    _conf__task_family = "dynamic_task"

    enabled = parameter(default=True, description="Allow tasks calls at runtime")[bool]
    in_memory_outputs = parameter(
        default=False, description="Store outputs for inline tasks in memory"
    )[bool]


class FeaturesConfig(Config):
    """
    Configuration section for caching features.
    """

    _conf__task_family = "features"

    in_memory_cache_target_value = parameter(
        default=True, description="Cache targets values in memory during execution"
    )[bool]

    log_value_size = parameter(
        default=True,
        description="Calculate and log value size (can cause a full scan on not-indexable distributed memory objects) ",
    )[bool]

    log_value_schema = parameter(
        default=True, description="Calculate and log value schema "
    )[bool]
    log_value_stats = parameter(
        default=True,
        description="Calculate and log value stats(expensive to calculate, better use log_stats on parameter level)",
    )[bool]
    log_value_preview = parameter(
        default=True, description="Calculate and log value preview "
    )[bool]

    log_value_preview_max_len = parameter(
        description="Max size of value preview to be saved at DB, max value=50000"
    ).value(_DEFAULT_VALUE_PREVIEW_MAX_LEN)

    log_value_meta = parameter(
        default=True, description="Calculate and log value meta "
    )[bool]

    log_df_histograms = parameter(
        default=True, description="Calculate and log DataFrames histograms"
    )[bool]

    auto_disable_slow_size = parameter(
        default=True,
        description="Auto disable slow preview for Spark DF with text formats",
    )[bool]

    def get_value_meta_conf(
        self, parameter_value_meta_conf, value_type=None, target=None
    ):
        # type: (ValueMetaConf, ValueType, Target) -> ValueMetaConf
        mc = parameter_value_meta_conf

        log_value_size = self.log_value_size
        if target and self.auto_disable_slow_size and log_value_size:
            log_value_size = value_type.support_fast_count(target)

        return ValueMetaConf(
            log_preview=mc.log_preview
            if mc.log_preview is not None
            else self.log_value_preview,
            log_preview_size=mc.log_preview_size
            if mc.log_preview_size is not None
            else self.log_value_preview_max_len,
            log_schema=mc.log_schema
            if mc.log_schema is not None
            else self.log_value_schema,
            log_size=mc.log_size if mc.log_size is not None else log_value_size,
            log_stats=mc.log_stats,
            log_df_hist=bool(self.log_df_histograms and mc.log_df_hist),
        )
