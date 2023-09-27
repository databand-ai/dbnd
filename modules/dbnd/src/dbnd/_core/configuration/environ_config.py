# © Copyright Databand.ai, an IBM Company 2022
#  << should be run before import to airflow >>
# otherwise airflow.configuration will fail
# fix AIRFLOW_HOME for all runs

import logging
import os

from contextlib import contextmanager
from typing import Optional

from dbnd._core.log.dbnd_log import dbnd_log_init_msg
from dbnd._core.utils.basics.environ_utils import (
    environ_enabled,
    environ_int,
    set_off,
    set_on,
)
from dbnd._core.utils.basics.path_utils import abs_join


logger = logging.getLogger(__name__)

DATABAND_AIRFLOW_CONN_ID = "dbnd_config"  # DBND connection ID for Airflow connections
PARAM_ENV_TEMPLATE = "DBND__{S}__{K}"

ENV_DBND__DISABLED = "DBND__DISABLED"
ENV_DBND__TRACKING = (
    "DBND__TRACKING"  # implicit DBND tracking ( on any @task/log_ call)
)
ENV_DBND__UNITTEST_MODE = "DBND__UNITTEST"

ENV_DBND_HOME = "DBND_HOME"
ENV_DBND_SYSTEM = "DBND_SYSTEM"
ENV_DBND_LIB = "DBND_LIB"
ENV_DBND_CONFIG = "DBND_CONFIG"  # extra config for DBND

# Orchestration values

ENV_DBND__ORCHESTRATION_MODE = (
    "DBND__ORCHESTRATION_MODE"  # disable orchestration even if installed
)
# do not auto-load orchestration user plugins (for example all dbnd-tensorflow)
ENV_DBND__ORCHESTRATION__NO_PLUGINS = "DBND__ORCHESTRATION__NO_PLUGINS"

ENV_DBND__USER_PRE_INIT = "DBND__USER_PRE_INIT"  # run on user init
ENV_DBND__RUN__PLUGINS = "DBND__RUN__PLUGINS"
ENV_DBND_FIX_PYSPARK_IMPORTS = "DBND__FIX_PYSPARK_IMPORTS"
ENV_DBND__DISABLE_PLUGGY_ENTRYPOINT_LOADING = "DBND__DISABLE_PLUGGY_ENTRYPOINT_LOADING"
# EO orchestration

ENV_DBND__NO_TABLES = "DBND__NO_TABLES"  # do not print fancy tables
ENV_DBND__SHOW_STACK_ON_SIGQUIT = "DBND__SHOW_STACK_ON_SIGQUIT"
ENV_DBND__DISABLE_AIRFLOW_SUBDAG_TRACKING = "DBND__DISABLE_AIRFLOW_SUBDAG_TRACKING"

ENV_DBND_USER = "DBND_USER"
ENV_DBND_ENV = "DBND_ENV"

# DBND RUN info variables
SCHEDULED_DAG_RUN_ID_ENV = "SCHEDULED_DAG_RUN_ID"
SCHEDULED_DATE_ENV = "SCHEDULED_DATE"
SCHEDULED_JOB_UID_ENV = "SCHEDULED_JOB_UID"
SCHEDULED_JOB_NAME_ENV = "SCHEDULED_JOB_NAME"
DBND_ROOT_RUN_UID = "DBND_ROOT_RUN_UID"
DBND_ROOT_RUN_TRACKER_URL = "DBND_ROOT_RUN_TRACKER_URL"
DBND_PARENT_TASK_RUN_UID = "DBND_PARENT_TASK_RUN_UID"
DBND_PARENT_TASK_RUN_ATTEMPT_UID = "DBND_PARENT_TASK_RUN_ATTEMPT_UID"
DBND_RUN_SUBMIT_UID = "DBND_SUBMIT_UID"
DBND_RUN_UID = "DBND_RUN_UID"
DBND_RESUBMIT_RUN = "DBND_RESUBMIT_RUN"
DBND_TASK_RUN_ATTEMPT_UID = "DBND_TASK_RUN_ATTEMPT_UID"
DBND_TRACE_ID = "DBND_TRACE_ID"
DBND_MAX_CALLS_PER_RUN = "DBND_MAX_CALL_PER_FUNC"
ENV_DBND_DISABLE_SCHEDULED_DAGS_LOAD = "DBND_DISABLE_SCHEDULED_DAGS_LOAD"

ENV_DBND__ENV_MACHINE = "DBND__ENV_MACHINE"
ENV_DBND__ENV_IMAGE = "DBND__ENV_IMAGE"

ENV_SHELL_COMPLETION = "_DBND_COMPLETE"

ENV_DBND__ENABLE__SPARK_CONTEXT_ENV = "DBND__ENABLE__SPARK_CONTEXT_ENV"

ENV_DBND__AUTO_TRACKING = "DBND__AUTO_TRACKING"

DEFAULT_MAX_CALLS_PER_RUN = 100

ENV_DBND_TRACKING_ATTEMPT_UID = "DBND__TRACKING_ATTEMPT_UID"

ENV_DBND_SCRIPT_NAME = "DBND__SCRIPT_NAME"


_dbnd_enabled = True


def is_dbnd_enabled():
    return _dbnd_enabled


def is_dbnd_disabled():
    return not _dbnd_enabled


def disable_dbnd():
    """Disable dbnd tracking / wrapping / any effect on the user code."""
    global _dbnd_enabled
    if _dbnd_enabled:
        logger.info("DBND has been disabled per user request.")
    _dbnd_enabled = False
    set_on(ENV_DBND__DISABLED)
    logger.info("DBND has been disabled per user request.")


def enable_dbnd():
    """
    Enable dbnd system ( run only after you have used dbnd disabled )
    """
    global _dbnd_enabled
    _dbnd_enabled = True

    set_off(ENV_DBND__DISABLED)
    logger.info("Databand has been enabled per user request")


## WE NEED TO INIALIZE now
if environ_enabled(ENV_DBND__DISABLED):
    disable_dbnd()


_unit_test_mode = environ_enabled(ENV_DBND__UNITTEST_MODE)


def is_unit_test_mode():
    return _unit_test_mode


def set_dbnd_unit_test_mode():
    global _unit_test_mode

    set_on(ENV_DBND__UNITTEST_MODE)  # bypass to subprocess
    _unit_test_mode = True


def get_max_calls_per_func():
    return get_dbnd_project_config().max_calls_per_run


# User setup configs
def get_dbnd_environ_config_file():
    return os.environ.get(ENV_DBND_CONFIG, None)


def get_user_preinit():
    return os.environ.get(ENV_DBND__USER_PRE_INIT, None)


def is_inplace_tracking_mode():
    return get_dbnd_project_config().is_inplace_tracking_mode()


def spark_tracking_enabled():
    return environ_enabled(ENV_DBND__ENABLE__SPARK_CONTEXT_ENV)


def should_fix_pyspark_imports():
    return environ_enabled(ENV_DBND_FIX_PYSPARK_IMPORTS)


_project_config = None  # type: Optional[DbndProjectConfig]


def get_dbnd_project_config():
    global _project_config
    if not _project_config:
        # initialize dbnd home first
        dbnd_log_init_msg("New project config")
        _project_config = DbndProjectConfig()
    return _project_config


def reset_dbnd_project_config():
    global _project_config
    _project_config = None


@contextmanager
def tracking_mode_context(tracking=None):
    """
    change the tracking mode for the scope of the `with`
    """
    is_current_tracking = get_dbnd_project_config()._dbnd_inplace_tracking
    get_dbnd_project_config()._dbnd_inplace_tracking = tracking
    try:
        yield
    finally:
        get_dbnd_project_config()._dbnd_inplace_tracking = is_current_tracking


class DbndProjectConfig(object):
    """
    very basic environment config!
    """

    def __init__(self):
        self.max_calls_per_run = environ_int(
            DBND_MAX_CALLS_PER_RUN, DEFAULT_MAX_CALLS_PER_RUN
        )

        self.shell_cmd_complete_mode = ENV_SHELL_COMPLETION in os.environ
        if self.shell_cmd_complete_mode:
            # we should not print anything if we are in shell completion!
            import logging

            logging.getLogger().setLevel(logging.CRITICAL + 1)

        # TRACKING MODE
        self._dbnd_inplace_tracking = environ_enabled(ENV_DBND__TRACKING, default=None)
        self.airflow_auto_tracking = environ_enabled(
            ENV_DBND__AUTO_TRACKING, default=True
        )

        # ORCHESTRATION MODE
        self._dbnd_orchestration = environ_enabled(ENV_DBND__ORCHESTRATION_MODE)

        # external process can create "wrapper run"  (airflow scheduler)
        # a run with partial information,
        # when we have a subprocess,  only nested run will have all actual details
        # so we are going to "resubmit" them
        self.resubmit_run = (
            DBND_RESUBMIT_RUN in os.environ
            and os.environ.pop(DBND_RESUBMIT_RUN) == "true"
        )

        self.is_no_plugins = environ_enabled(ENV_DBND__ORCHESTRATION__NO_PLUGINS)
        self.disable_pluggy_entrypoint_loading = environ_enabled(
            ENV_DBND__DISABLE_PLUGGY_ENTRYPOINT_LOADING
        )

        self.is_sigquit_handler_on = environ_enabled(ENV_DBND__SHOW_STACK_ON_SIGQUIT)

        # we use DBND_HOME and DBND_SYSTEM
        _initialize_dbnd_home()
        dbnd_log_init_msg("DbndProjectConfig has been created.")

    def is_inplace_tracking_mode(self):
        return self._dbnd_inplace_tracking

    def dbnd_home(self):
        return os.environ.get(ENV_DBND_HOME) or os.curdir

    def dbnd_lib_path(self, *path):
        return abs_join(os.environ.get(ENV_DBND_LIB), *path)

    def dbnd_config_path(self, *path):
        return self.dbnd_lib_path("conf", *path)

    def dbnd_system_path(self, *path):
        dbnd_system = os.environ.get(ENV_DBND_SYSTEM) or self.dbnd_home()
        return abs_join(dbnd_system, *path)

    def dbnd_project_path(self, *path):
        return abs_join(self.dbnd_home(), *path)

    def set_orchestration_mode(self):
        self._dbnd_orchestration = True

    def is_orchestration_mode(self):
        return self._dbnd_orchestration


_DBND_ENVIRONMENT = False


def _initialize_dbnd_home():
    global _DBND_ENVIRONMENT
    if _DBND_ENVIRONMENT:
        return
    _DBND_ENVIRONMENT = True

    if is_dbnd_disabled():
        dbnd_log_init_msg("databand is disabled, no DBND_HOME setup")
        return

    dbnd_log_init_msg("Initializing DBND Home")
    _initialize_google_composer()

    # main logic
    from dbnd._core.configuration.environ_dbnd_home import (
        __initialize_dbnd_home_environ,
    )

    __initialize_dbnd_home_environ()


def _initialize_google_composer():
    if "COMPOSER_ENVIRONMENT" not in os.environ:
        return
    dbnd_log_init_msg("Initializing Google Composer Environment")

    if ENV_DBND_HOME not in os.environ:
        os.environ[ENV_DBND_HOME] = os.environ["HOME"]

    env_tracker_raise_on_error = "DBND__CORE__TRACKER_RAISE_ON_ERROR"
    if env_tracker_raise_on_error not in os.environ:
        os.environ[env_tracker_raise_on_error] = "false"


def dbnd_project_path(*path):
    return get_dbnd_project_config().dbnd_project_path(*path)
