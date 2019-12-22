import os
import sys


PARAM_ENV_TEMPLATE = "DBND__{S}__{K}"

ENV_DBND_HOME = "DBND_HOME"
ENV_AIRFLOW_CONFIG = "AIRFLOW_CONFIG"
ENV_DBND_SYSTEM = "DBND_SYSTEM"
ENV_DBND_LIB = "DBND_LIB"


ENV_DBND__ENABLED = "DBND__ENABLED"
ENV_DBND__UNITTEST_MODE = "DBND__UNITTEST_MODE"
ENV_DBND__CONFIG = "DBND__CONFIG"
ENV_DBND__USER_PRE_INIT = "DBND__USER_PRE_INIT"

ENV_DBND_USER = "DBND_USER"

# DBND RUN info variables
SCHEDULED_DAG_RUN_ID_ENV = "SCHEDULED_DAG_RUN_ID"
SCHEDULED_DATE_ENV = "SCHEDULED_DATE"
SCHEDULED_JOB_UID_ENV = "SCHEDULED_JOB_UID"
DBND_ROOT_RUN_UID = "DBND_ROOT_RUN_UID"
DBND_ROOT_RUN_TRACKER_URL = "DBND_ROOT_RUN_TRACKER_URL"
DBND_PARENT_TASK_RUN_UID = "DBND_PARENT_TASK_RUN_UID"
DBND_RUN_SUBMIT_UID = "DBND_SUBMIT_UID"
DBND_RUN_UID = "DBND_RUN_UID"
DBND_RESUBMIT_RUN = "DBND_RESUBMIT_RUN"

# IF FALSE  - we will not modify decorated @task code
DBND_ENABLED = None

ENV_SHELL_COMPLETION = "_DBND_COMPLETE"
_SHELL_COMPLETION = ENV_SHELL_COMPLETION in os.environ or "--help" in sys.argv


def is_shell_cmd_complete_mode():
    return _SHELL_COMPLETION


def environ_enabled(variable_name, default=False):
    env_value = os.environ.get(variable_name, None)
    if env_value is None:
        return default

    from dbnd._core.utils.basics.helpers import parse_bool

    return parse_bool(env_value)


def get_dbnd_environ_config_file():
    return os.environ.get(ENV_DBND__CONFIG, None)


def is_unit_test_mode():
    return environ_enabled(ENV_DBND__UNITTEST_MODE)


def is_databand_enabled():
    global DBND_ENABLED
    if DBND_ENABLED is not None:
        return DBND_ENABLED
    DBND_ENABLED = environ_enabled(ENV_DBND__ENABLED, True)
    return DBND_ENABLED


def disable_databand():
    global DBND_ENABLED
    DBND_ENABLED = False


def set_dbnd_unit_test_mode():
    os.environ[ENV_DBND__UNITTEST_MODE] = "True"


def get_user_preinit():
    return os.environ.get(ENV_DBND__USER_PRE_INIT, None)
