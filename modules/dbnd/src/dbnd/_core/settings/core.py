# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict, List

from dbnd._core.configuration.config_readers import read_from_config_files
from dbnd._core.constants import CloudType
from dbnd._core.current import is_verbose
from dbnd._core.log import dbnd_log_debug
from dbnd._core.parameter import PARAMETER_FACTORY as parameter
from dbnd._core.task import Config


logger = logging.getLogger()


class DatabandSystemConfig(Config):
    """Databand's command line arguments (see `dbnd run --help`)"""

    _conf__task_family = "databand"

    verbose = parameter(description="Make logging output more verbose").value(False)

    describe = parameter.help("Describe current run").value(False)

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

    def update_config_from_user_inputs(self, config):
        if self.conf:
            config.set_values(self.conf, source="[databand]conf")
        if self.conf_file:
            conf_file = read_from_config_files(self.conf_file)
            config.set_values(conf_file, source="[databand]conf")


class CoreConfig(Config):
    """Databand's core functionality behaviour"""

    _conf__task_family = "core"

    databand_url = parameter(
        default=None,
        description="Set the tracker URL which will be used for creating links in console logs.",
    )[str]
    databand_access_token = parameter(
        description="Set the personal access token used to connect to the Databand web server.",
        default=None,
        hidden=True,
    )[str]

    ignore_ssl_errors = parameter(
        description="Ignore SSL errors to allow usage of the self-signed certificate on dbnd web server",
        default=False,
    )[bool]

    extra_default_headers = parameter(
        description="Specify extra headers to be used as defaults for databand_api_client.",
        default=None,
    )[Dict[str, str]]

    tracker = parameter(description="Set the Tracking Stores to be used.")[List[str]]

    tracker_api = parameter(
        default="web", description="Set the Tracker Channels to be used by 'api' store."
    )[str]

    tracker_version = parameter[str]

    debug_webserver = parameter(
        description="Enable collecting the webserver's logs for each api-call on the local machine. "
        "This requires that the web-server supports and allows this.",
        default=False,
    )[bool]

    tracker_raise_on_error = parameter(
        default=True, description="Enable raising an error when failed to track data."
    )[bool]

    remove_failed_store = parameter(
        default=False, description="Enable removal of a tracking store if it fails."
    )[bool]

    max_tracking_store_retries = parameter(
        default=2,
        description="Set maximum amount of retries allowed for a single tracking store call if it fails.",
    )[int]

    client_session_timeout = parameter(
        description="Set number of minutes to recreate the api client's session.",
        default=5,
    )[int]
    client_max_retry = parameter(
        description="Set the maximum amount of retries on failed connection for the api client.",
        default=2,
    )[int]
    client_retry_sleep = parameter(
        description="Set the amount of sleep time in between retries of the API client.",
        default=0.1,
    )[float]

    #### TO DEPRECATE ## (DONT USE)
    # deprecate in favor of databand_access_token
    dbnd_user = parameter(
        description="Set which user should be used to connect to the DBND web server. This is deprecated!"
    )[str]
    dbnd_password = parameter(
        description="Set what password should be used to connect to the DBND web server. This is deprecated!",
        hidden=True,
    )[str]

    def _validate(self):
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
            dbnd_log_debug(
                "core.databand_access_token is used instead of defined dbnd_user and dbnd_password."
            )

        # in tracking mode we track to console only if we are not tracking to the api
        if is_verbose() and "console" not in self.tracker:
            dbnd_log_debug("Adding `console` tracker as verbose is enabled.")
            self.tracker.append("console")

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
            ignore_ssl_errors=self.ignore_ssl_errors,
        )
