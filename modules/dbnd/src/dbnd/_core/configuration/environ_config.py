import os

from typing import Optional


PARAM_ENV_TEMPLATE = "DBND__{S}__{K}"

# Filee locations
ENV_DBND_HOME = "DBND_HOME"
ENV_AIRFLOW_CONFIG = "AIRFLOW_CONFIG"
ENV_DBND_SYSTEM = "DBND_SYSTEM"
ENV_DBND_LIB = "DBND_LIB"
ENV_DBND_CONFIG = "DBND_CONFIG"  # extra config for DBND


ENV_DBND__ENABLED = "DBND__ENABLED"
ENV_DBND__TRACKING = (
    "DBND__TRACKING"  # implicit DBND tracking ( on any @task/log_ call)
)
ENV_DBND__VERBOSE = "DBND__DATABAND__VERBOSE"  # VERBOSE
ENV_DBND__UNITTEST_MODE = "DBND__UNITTEST_MODE"
ENV_DBND__USER_PRE_INIT = "DBND__USER_PRE_INIT"  # run on user init
ENV_DBND__NO_MODULES = "DBND__NO_MODULES"  # do not auto-load user modules

ENV_DBND__NO_TABLES = "DBND__NO_TABLES"  # do not print fancy tables
ENV_DBND__SHOW_STACK_ON_SIGQUIT = "DBND__SHOW_STACK_ON_SIGQUIT"
ENV_DBND__OVERRIDE_AIRFLOW_LOG_SYSTEM_FOR_TRACKING = (
    "DBND__OVERRIDE_AIRFLOW_LOG_SYSTEM_FOR_TRACKING"
)
ENV_DBND__DISABLE_AIRFLOW_SUBDAG_TRACKING = "DBND__DISABLE_AIRFLOW_SUBDAG_TRACKING"

ENV_DBND_USER = "DBND_USER"
ENV_DBND_ENV = "DBND_ENV"

# DBND RUN info variables
SCHEDULED_DAG_RUN_ID_ENV = "SCHEDULED_DAG_RUN_ID"
SCHEDULED_DATE_ENV = "SCHEDULED_DATE"
SCHEDULED_JOB_UID_ENV = "SCHEDULED_JOB_UID"
DBND_ROOT_RUN_UID = "DBND_ROOT_RUN_UID"
DBND_ROOT_RUN_TRACKER_URL = "DBND_ROOT_RUN_TRACKER_URL"
DBND_PARENT_TASK_RUN_UID = "DBND_PARENT_TASK_RUN_UID"
DBND_PARENT_TASK_RUN_ATTEMPT_UID = "DBND_PARENT_TASK_RUN_ATTEMPT_UID"
DBND_RUN_SUBMIT_UID = "DBND_SUBMIT_UID"
DBND_RUN_UID = "DBND_RUN_UID"
DBND_RESUBMIT_RUN = "DBND_RESUBMIT_RUN"
DBND_TASK_RUN_ATTEMPT_UID = "DBND_TASK_RUN_ATTEMPT_UID"
DBND_MAX_CALLS_PER_RUN = "DBND_MAX_CALL_PER_FUNC"
DEFAULT_MAX_CALLS_PER_RUN = 100


ENV_DBND__ENV_MACHINE = "DBND__ENV_MACHINE"
ENV_DBND__ENV_IMAGE = "DBND__ENV_IMAGE"

ENV_DBND_DISABLE_SCHEDULED_DAGS_LOAD = "DBND_DISABLE_SCHEDULED_DAGS_LOAD"
ENV_SHELL_COMPLETION = "_DBND_COMPLETE"

ENV_DBND_QUIET = "DBND_QUIET"


class BasicEnvironConfig(object):
    """
    very basic environment config!
    """

    def __init__(self):
        # IF FALSE  - we will not modify decorated @task code
        self.enabled = environ_enabled(ENV_DBND__ENABLED, True)

        self.unit_test_mode = environ_enabled(ENV_DBND__UNITTEST_MODE)

        self.max_calls_per_run = environ_int(
            DBND_MAX_CALLS_PER_RUN, DEFAULT_MAX_CALLS_PER_RUN
        )

        self.shell_cmd_complete_mode = ENV_SHELL_COMPLETION in os.environ
        self.quiet_mode = (
            os.environ.pop(ENV_DBND_QUIET, None) is not None
            or self.shell_cmd_complete_mode
        )

        self._verbose = environ_enabled(ENV_DBND__VERBOSE)

        self._dbnd_tracking = environ_enabled(ENV_DBND__TRACKING)

        self._airflow_context = False
        self._inline_tracking = None

        self.disable_inline = False

    def airflow_context(self):
        if self._airflow_context is False:
            from dbnd._core.inplace_run.airflow_dag_inplace_tracking import (
                try_get_airflow_context,
            )

            self._airflow_context = try_get_airflow_context()
        return self._airflow_context

    def is_inplace_run(self):
        if not self.enabled:
            return False
        return self._dbnd_tracking or self.airflow_context()

    def is_verbose(self):
        from dbnd._core.current import try_get_databand_context

        context = try_get_databand_context()
        if context and getattr(context, "system_settings", None):
            return context.system_settings.verbose

        return self._verbose


def set_quiet_mode(msg="no extra logs"):
    get_environ_config().quiet_mode = False


def in_shell_cmd_complete_mode():
    return get_environ_config().shell_cmd_complete_mode


def in_quiet_mode():
    """
    quiet mode was made for the scheduler to silence the launcher runners.
    Don't want this flag to propagate into the actual scheduled cmd
    """
    return get_environ_config().quiet_mode


def is_unit_test_mode():
    return get_environ_config().unit_test_mode


def is_databand_enabled():
    return get_environ_config().enabled


def disable_databand():
    get_environ_config().enabled = False


def set_dbnd_unit_test_mode():
    set_on(ENV_DBND__UNITTEST_MODE)  # bypass to subprocess
    get_environ_config().unit_test_mode = True


def get_max_calls_per_func():
    return get_environ_config().max_calls_per_run


# User setup configs


def get_dbnd_environ_config_file():
    return os.environ.get(ENV_DBND_CONFIG, None)


def get_user_preinit():
    return os.environ.get(ENV_DBND__USER_PRE_INIT, None)


def is_no_modules():
    return environ_enabled(ENV_DBND__NO_MODULES)


def is_sigquit_handler_on():
    return environ_enabled(ENV_DBND__SHOW_STACK_ON_SIGQUIT)


def environ_enabled(variable_name, default=False):
    # type: (str, bool) -> bool
    env_value = os.environ.get(variable_name, None)
    if env_value is None:
        return default

    from dbnd._core.utils.basics.helpers import parse_bool

    return parse_bool(env_value)


def environ_int(variable_name, default=None):
    # type: (str, Optional[int]) -> Optional[int]
    env_value = os.environ.get(variable_name, None)
    if env_value is None:
        return default
    try:
        return int(env_value)
    except:
        return default


def set_on(env_key):
    os.environ[env_key] = "True"


_environ_config = None  # type: Optional[BasicEnvironConfig]


def get_environ_config():
    global _environ_config
    if not _environ_config:
        _environ_config = BasicEnvironConfig()
    return _environ_config


def reset():
    global _environ_config
    _environ_config = None
