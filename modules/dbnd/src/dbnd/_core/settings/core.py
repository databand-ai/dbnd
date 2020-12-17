import logging

from typing import Dict, List

from dbnd._core.constants import CloudType
from dbnd._core.log import dbnd_log_debug
from dbnd._core.parameter import PARAMETER_FACTORY as parameter
from dbnd._core.task import Config
from dbnd._core.utils.function_args import first_not_none
from targets import Target
from targets.value_meta import _DEFAULT_VALUE_PREVIEW_MAX_LEN, ValueMetaConf
from targets.values import ValueType


logger = logging.getLogger()


class DatabandSystemConfig(Config):
    """Databand's command line arguments (see `dbnd run --help`)"""

    _conf__task_family = "databand"

    verbose = parameter(description="Make logging output more verbose").value(False)
    print_task_band = parameter(description="Print task_band in logging output.").value(
        False
    )

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

    dbnd_user = parameter(description="user used to connect to the dbnd web server")[
        str
    ]
    dbnd_password = parameter(
        description="password used to connect to the dbnd web server"
    )[str]
    databand_url = parameter(
        default=None,
        description="Tracker URL to be used for creating links in console logs",
    )[str]
    databand_personal_access_token = parameter(
        description="Personall access token to connect to the dbnd web server",
        default=None,
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
    allow_vendored_package = parameter(
        description="Allow adding dbnd/_vendor_package to sys.path", default=False
    )[bool]

    def _validate(self):
        if not self.databand_url and self.tracker_url:
            logger.warning(
                "core.databand_url was not set, using deprecated 'core.tracker_url' instead."
            )
            self.databand_url = self.tracker_url

        if self.databand_url and self.databand_url.endswith("/"):
            dbnd_log_debug(
                "Please fix your core.databand_url value, "
                "it should not contain '/' at the end, auto-fix has been applied."
            )
            self.databand_url = self.databand_url[:-1]

        # automatically disabling tracking if databand_url is not set
        if not self.databand_url:
            dbnd_log_debug(
                "Automatically disabling tracking to databand service as databand_url is not set"
            )
            self.tracker = [t for t in self.tracker if t != "api"]

        if self.databand_personal_access_token and (
            self.dbnd_user or self.dbnd_password
        ):
            logger.warning(
                "core.databand_personal_access_token is used instead of defined dbnd_user and dbnd_password."
            )

    def build_tracking_store(self, remove_failed_store=True):
        from dbnd._core.tracking.registry import get_tracking_store

        return get_tracking_store(
            tracking_store_names=self.tracker,
            api_channel_name=self.tracker_api,
            tracker_raise_on_error=self.tracker_raise_on_error,
            remove_failed_store=remove_failed_store,
        )

    def build_databand_api_client(self):
        from dbnd.utils.api_client import ApiClient

        credentials = (
            {"token": self.databand_personal_access_token}
            if self.databand_personal_access_token
            else {"username": self.dbnd_user, "password": self.dbnd_password}
        )

        return ApiClient(self.databand_url, credentials=credentials)


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


class TrackingConfig(Config):
    _conf__task_family = "tracking"

    databand_external_url = parameter(
        default=None,
        description="Tracker URL to be used for tracking from external systems",
    )[str]

    log_value_size = parameter(
        default=True,
        description="Calculate and log value size (can cause a full scan on not-indexable distributed memory objects) ",
    )[bool]

    log_value_schema = parameter(
        default=True, description="Calculate and log value schema "
    )[bool]
    log_value_stats = parameter(
        default=False,
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

    log_histograms = parameter(
        default=False,
        description="Enable calculation and tracking of histograms. Can be expensive",
    )[bool]

    auto_disable_slow_size = parameter(
        default=True,
        description="Auto disable slow preview for Spark DF with text formats",
    )[bool]

    flatten_operator_fields = parameter(
        default={},
        description="Control which of the operator's fields would be flatten when tracked",
    )[Dict[str, str]]

    def get_value_meta_conf(
        self, parameter_value_meta_conf, value_type=None, target=None
    ):
        # type: (ValueMetaConf, ValueType, Target) -> ValueMetaConf
        mc = parameter_value_meta_conf

        log_value_size = self.log_value_size
        if target and self.auto_disable_slow_size and log_value_size:
            log_value_size = value_type.support_fast_count(target)

        return ValueMetaConf(
            log_preview=first_not_none(mc.log_preview, self.log_value_preview),
            log_preview_size=first_not_none(
                mc.log_preview_size, self.log_value_preview_max_len
            ),
            log_schema=first_not_none(mc.log_schema, self.log_value_schema),
            log_size=first_not_none(mc.log_size, log_value_size),
            log_stats=first_not_none(mc.log_stats, self.log_value_stats),
            log_histograms=first_not_none(mc.log_histograms, self.log_histograms),
        )
