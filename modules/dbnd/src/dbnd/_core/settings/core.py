import logging

from typing import Dict, List

from dbnd._core.configuration.environ_config import in_tracking_mode
from dbnd._core.constants import CloudType
from dbnd._core.log import dbnd_log_debug
from dbnd._core.parameter import PARAMETER_FACTORY as parameter
from dbnd._core.task import Config


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
    databand_access_token = parameter(
        description="Personal access token to connect to the dbnd web server",
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
        default=None, description="Runs in sub process (parallel/kubernetes/external)"
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
    remove_failed_store = parameter(
        default=False, description="Remove a tracking store if it fails"
    )[bool]
    max_tracking_store_retries = parameter(
        default=2,
        description="Maximal amounts of retries allowed for a single tracking store call if it failed.",
    )[int]

    tracker_api = parameter(default="web", description="Tracking Stores to be used")[
        str
    ]

    silence_tracking_mode = parameter(
        default=False, description="Silence console on tracking mode"
    )[bool]

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

    debug_webserver = parameter(
        description="Allow collecting the webservers logs for each api-call on the local machine. "
        "Requires that the web-server supports and allow this.",
        default=False,
    )[bool]

    client_session_timeout = parameter(
        description="Minutes to recreate the api client's session", default=5
    )[int]
    client_max_retry = parameter(
        description="Maximum amount of retries on failed connection for the api client",
        default=2,
    )[int]
    client_retry_sleep = parameter(
        description="Sleep between retries of the api client", default=0.1
    )[float]

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

        if self.databand_access_token and (self.dbnd_user or self.dbnd_password):
            logger.warning(
                "core.databand_access_token is used instead of defined dbnd_user and dbnd_password."
            )

        if not (in_tracking_mode() and "api" in self.tracker):
            self.silence_tracking_mode = False

        # in tracking mode we track to console only if we are not tracking to the api
        if self.silence_tracking_mode:
            self.tracker = [t for t in self.tracker if t != "console"]

    def build_tracking_store(self, remove_failed_store=None):
        from dbnd._core.tracking.registry import get_tracking_store

        if remove_failed_store is None:
            remove_failed_store = self.remove_failed_store

        return get_tracking_store(
            tracking_store_names=self.tracker,
            api_channel_name=self.tracker_api,
            max_retires=self.max_tracking_store_retries,
            tracker_raise_on_error=self.tracker_raise_on_error,
            remove_failed_store=remove_failed_store,
        )

    def build_databand_api_client(self):
        from dbnd.utils.api_client import ApiClient

        credentials = (
            {"token": self.databand_access_token}
            if self.databand_access_token
            else {"username": self.dbnd_user, "password": self.dbnd_password}
        )

        return ApiClient(
            self.databand_url,
            credentials=credentials,
            debug_server=self.debug_webserver,
            session_timeout=self.client_session_timeout,
            default_max_retry=self.client_max_retry,
            default_retry_sleep=self.client_retry_sleep,
        )


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
