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
        description="task environment: (based on core.environments) local/aws/aws_prod/gcp/prod",
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

    databand_url = parameter(
        default=None,
        description="Tracker URL to be used for creating links in console logs",
    )[str]
    databand_access_token = parameter(
        description="Personal access token to connect to the dbnd web server",
        default=None,
        hidden=True,
    )[str]
    extra_default_headers = parameter(
        description="Specify extra headers to be used as defaults for databand_api_client.",
        default=None,
    )[Dict[str, str]]

    tracker = parameter(
        default=["file", "console", "api"], description="Tracking Stores to be used"
    )[List[str]]

    tracker_api = parameter(
        default="web", description="Tracker Channels to be used by 'api' store"
    )[str]

    tracker_version = parameter[str]

    debug_webserver = parameter(
        description="Allow collecting the webservers logs for each api-call on the local machine. "
        "Requires that the web-server supports and allow this.",
        default=False,
    )[bool]

    silence_tracking_mode = parameter(
        default=False, description="Silence console on tracking mode"
    )[bool]

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

    # USER CODE TO RUN ON START
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

    # PLUGINS
    plugins = parameter(
        description="plugins to load on databand context creation", default=None
    )[str]
    allow_vendored_package = parameter(
        description="Allow adding dbnd/_vendor_package to sys.path", default=False
    )[bool]
    fix_env_on_osx = parameter(
        description="add no_proxy=* to env vars, fixing issues with multiprocessing on osx"
    )[bool]

    environments = parameter(description="List of enabled environments")[List[str]]

    #### TO DEPRECATE ## (DONT USE)
    # deprecate in favor of databand_access_token
    dbnd_user = parameter(
        description="DEPRECATED: user used to connect to the dbnd web server"
    )[str]
    dbnd_password = parameter(
        description="DEPRECATED: password used to connect to the dbnd web server",
        hidden=True,
    )[str]

    # deprecated at 0.34 (Backward compatibility)
    tracker_url = parameter(
        default=None,
        description="DEPRECATED: Tracker URL to be used for creating links in console logs",
    )[str]

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
            logger.debug(
                "core.databand_access_token is used instead of defined dbnd_user and dbnd_password."
            )

        if not (in_tracking_mode() and "api" in self.tracker):
            self.silence_tracking_mode = False

        # in tracking mode we track to console only if we are not tracking to the api
        if self.silence_tracking_mode:
            self.tracker = [t for t in self.tracker if t != "console"]

    def build_tracking_store(self, databand_ctx, remove_failed_store=None):
        from dbnd._core.tracking.registry import get_tracking_store

        if remove_failed_store is None:
            remove_failed_store = self.remove_failed_store

        return get_tracking_store(
            databand_ctx,
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
            extra_default_headers=self.extra_default_headers,
        )
