#  << should be run before import to airflow >>
# otherwise airflow.configuration will fail
# fix AIRFLOW_HOME for all runs

import os

from configparser import ConfigParser
from contextlib import contextmanager

from dbnd._core.configuration.project_env import (
    _init_windows_python_path,
    _is_init_mode,
)
from dbnd._core.utils.basics.environ_utils import (
    env,
    environ_enabled,
    environ_int,
    set_env_dir,
    set_on,
)
from dbnd._core.utils.basics.path_utils import abs_join, relative_path


_MARKER_FILES = ["databand.cfg", "project.cfg", "databand-system.cfg"]
PARAM_ENV_TEMPLATE = "DBND__{S}__{K}"

ENV_DBND__DISABLED = "DBND__DISABLED"
ENV_DBND__TRACKING = (
    "DBND__TRACKING"  # implicit DBND tracking ( on any @task/log_ call)
)
ENV_DBND__VERBOSE = "DBND__VERBOSE"  # VERBOSE
ENV_DBND__UNITTEST_MODE = "DBND__UNITTEST"
ENV_DBND_QUIET = "DBND__QUIET"

ENV_DBND_HOME = "DBND_HOME"
ENV_DBND_SYSTEM = "DBND_SYSTEM"
ENV_DBND_LIB = "DBND_LIB"
ENV_DBND_CONFIG = "DBND_CONFIG"  # extra config for DBND

ENV_DBND__DEBUG_INIT = "DBND__DEBUG_INIT"

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
ENV_DBND_DISABLE_SCHEDULED_DAGS_LOAD = "DBND_DISABLE_SCHEDULED_DAGS_LOAD"

ENV_DBND__ENV_MACHINE = "DBND__ENV_MACHINE"
ENV_DBND__ENV_IMAGE = "DBND__ENV_IMAGE"
ENV_DBND__CORE__PLUGINS = "DBND__CORE__PLUGINS"

ENV_SHELL_COMPLETION = "_DBND_COMPLETE"

ENV_DBND_FIX_PYSPARK_IMPORTS = "DBND__FIX_PYSPARK_IMPORTS"
ENV_DBND__DISABLE_PLUGGY_ENTRYPOINT_LOADING = "DBND__DISABLE_PLUGGY_ENTRYPOINT_LOADING"

ENV_DBND__AUTO_TRACKING = "DBND__AUTO_TRACKING"

DEFAULT_MAX_CALLS_PER_RUN = 100

ENV_DBND_TRACKING_ATTEMPT_UID = "DBND__TRACKING_ATTEMPT_UID"

_DBND_DEBUG_INIT = environ_enabled(ENV_DBND__DEBUG_INIT)
_databand_package = relative_path(__file__, "..", "..")


def is_databand_enabled():
    return not get_dbnd_project_config().disabled


def disable_databand():
    get_dbnd_project_config().disabled = True


def set_dbnd_unit_test_mode():
    set_on(ENV_DBND__UNITTEST_MODE)  # bypass to subprocess
    get_dbnd_project_config().unit_test_mode = True


def get_max_calls_per_func():
    return get_dbnd_project_config().max_calls_per_run


# User setup configs
def get_dbnd_environ_config_file():
    return os.environ.get(ENV_DBND_CONFIG, None)


def get_user_preinit():
    return os.environ.get(ENV_DBND__USER_PRE_INIT, None)


def in_quiet_mode():
    """
    quiet mode was made for the scheduler to silence the launcher runners.
    Don't want this flag to propagate into the actual scheduled cmd
    """
    return get_dbnd_project_config().quiet_mode


def is_unit_test_mode():
    return get_dbnd_project_config().unit_test_mode


def spark_tracking_enabled():
    return environ_enabled("DBND__ENABLE__SPARK_CONTEXT_ENV")


def should_fix_pyspark_imports():
    return environ_enabled(ENV_DBND_FIX_PYSPARK_IMPORTS)


_project_config = None  # type: Optional[DbndProjectConfig]


def get_dbnd_project_config():
    global _project_config
    if not _project_config:
        # initialize dbnd home first
        _project_config = DbndProjectConfig()
        _initialize_dbnd_home()
    return _project_config


def get_dbnd_custom_config():
    try:
        import dbnd_custom_config

        return dbnd_custom_config.get_config_file_path()
    except Exception:
        return ""


def reset_dbnd_project_config():
    global _project_config
    _project_config = None


@contextmanager
def new_dbnd_project_config_context(**environment):
    """
    Create new environment context and reset the project int it.
    """
    with env(**environment):
        reset_dbnd_project_config()
        yield get_dbnd_project_config()

    reset_dbnd_project_config()


class DbndProjectConfig(object):
    """
    very basic environment config!
    """

    def __init__(self):
        # IF FALSE  - we will not modify decorated @task code
        self._disabled = environ_enabled(ENV_DBND__DISABLED, False)
        self.unit_test_mode = environ_enabled(ENV_DBND__UNITTEST_MODE)

        self.max_calls_per_run = environ_int(
            DBND_MAX_CALLS_PER_RUN, DEFAULT_MAX_CALLS_PER_RUN
        )

        self.shell_cmd_complete_mode = ENV_SHELL_COMPLETION in os.environ
        self.quiet_mode = (
            os.environ.pop(ENV_DBND_QUIET, None) is not None
            or self.shell_cmd_complete_mode
        )

        self.is_no_modules = environ_enabled(ENV_DBND__NO_MODULES)
        self.disable_pluggy_entrypoint_loading = environ_enabled(
            ENV_DBND__DISABLE_PLUGGY_ENTRYPOINT_LOADING
        )
        self.is_sigquit_handler_on = environ_enabled(ENV_DBND__SHOW_STACK_ON_SIGQUIT)

        self._verbose = environ_enabled(ENV_DBND__VERBOSE)

        self._dbnd_tracking = environ_enabled(ENV_DBND__TRACKING, default=None)

        self._airflow_context = False
        self._inline_tracking = None

        self.disable_inline = False
        self.airflow_auto_tracking = environ_enabled(
            ENV_DBND__AUTO_TRACKING, default=True
        )

    @property
    def disabled(self):
        return self._disabled

    @disabled.setter
    def disabled(self, value):
        set_on(ENV_DBND__DISABLED)
        self._disabled = value

    def airflow_context(self):
        if not self._airflow_context:
            from dbnd._core.inplace_run.airflow_dag_inplace_tracking import (
                try_get_airflow_context,
            )

            self._airflow_context = try_get_airflow_context()
        return self._airflow_context

    def is_tracking_mode(self):
        if self.disabled:
            return False

        if self._dbnd_tracking is None:
            return bool(self.airflow_context())

        return self._dbnd_tracking

    def is_verbose(self):
        from dbnd._core.current import try_get_databand_context

        context = try_get_databand_context()
        if context and getattr(context, "system_settings", None):
            if context.system_settings.verbose:
                return True

        return self._verbose

    def dbnd_home(self):
        return os.environ.get(ENV_DBND_HOME) or os.curdir

    def dbnd_lib_path(self, *path):
        return abs_join(_databand_package, *path)

    def dbnd_config_path(self, *path):
        return self.dbnd_lib_path("conf", *path)

    def dbnd_system_path(self, *path):
        dbnd_system = os.environ.get(ENV_DBND_SYSTEM) or self.dbnd_home()
        return abs_join(dbnd_system, *path)

    def dbnd_project_path(self, *path):
        return abs_join(self.dbnd_home(), *path)

    def validate_init(self):
        _debug_init_print("Successfully created dbnd project config")


def _debug_init_print(msg):
    if _DBND_DEBUG_INIT:
        print("DBND INIT: %s" % msg)


class DatabandHomeError(Exception):
    pass


def _find_project_by_import():
    """
    check if we can have project marker file by import it
    """
    try:
        import _databand_project

        return abs_join(_databand_project.__file__, "..")
    except ImportError:
        _debug_init_print("Can't import `_databand_project` marker.")
    return None


def _process_cfg(folder):
    # dbnd home is being pointed inside [databand] in 'config' files
    found_dbnd_home = False
    config_file = None

    config_files = ["tox.ini", "setup.cfg"]
    for file in config_files:
        config_path = os.path.join(folder, file)
        try:
            parser = ConfigParser()
            parser.read(config_path)

            config_root, config_name = os.path.split(config_path)
            source = os.path.basename(config_path)
            if not parser.has_section("databand"):
                continue

            for config_key in ["dbnd_home", "dbnd_system", "dbnd_config"]:
                # TODO: hidden magic, do we need these setters?
                if not parser.has_option("databand", config_key):
                    continue
                config_value = parser.get("databand", config_key)
                config_value = os.path.abspath(os.path.join(config_root, config_value))
                set_env_dir(config_key, config_value)
                _debug_init_print("%s: %s=%s" % (source, config_key, config_value))

        except Exception as ex:
            print("Failed to process %s: %s" % (config_path, ex))

    return found_dbnd_home, config_file


def _has_marker_file(folder):
    # dbnd home is where 'marker' files are located in
    for file in _MARKER_FILES:
        file_path = os.path.join(folder, file)
        if os.path.exists(file_path):
            return folder, file_path

    return False, None


def __find_dbnd_home_at(folder):
    dbnd_home, config_file = _process_cfg(folder)
    if dbnd_home:
        _debug_init_print(
            "Found dbnd home by at %s, by config file: %s" % (dbnd_home, config_file)
        )
        return dbnd_home

    dbnd_home, marker_file = _has_marker_file(folder)
    if dbnd_home:
        _debug_init_print(
            "Found dbnd home by at %s, by marker file: %s" % (dbnd_home, marker_file)
        )
        return dbnd_home

    _debug_init_print("dbnd home was not found at %s" % folder)
    return False


def _find_and_set_dbnd_home():
    # falling back to simple version
    if _is_init_mode():
        dbnd_home = os.path.abspath(".")
        print("Initializing new dbnd environment, using %s as DBND_HOME" % dbnd_home)
        set_env_dir(ENV_DBND_HOME, dbnd_home)
        return True

    # looking for dbnd project folder to be set as home
    project_folder = _find_project_by_import()
    if project_folder:
        _debug_init_print(
            "Found project folder by import from marker %s" % project_folder
        )
        # we know about project folder, let try to find "custom" configs in it
        dbnd_home = __find_dbnd_home_at(project_folder)
        if not dbnd_home:
            _debug_init_print(
                "No markers at %s! setting dbnd_home to %s" % (dbnd_home, dbnd_home)
            )
            dbnd_home = project_folder

        set_env_dir(ENV_DBND_HOME, dbnd_home)
        return True

    # traversing all the way up to until finding relevant anchor files
    cur_dir = os.path.normpath(os.getcwd())
    cur_dir_split = cur_dir.split(os.sep)
    cur_dir_split_reversed = reversed(list(enumerate(cur_dir_split)))
    _debug_init_print(
        "Trying to find dbnd_home by traversing up to the root folder starting at %s"
        % cur_dir
    )

    for idx, cur_folder in cur_dir_split_reversed:
        cur_path = os.path.join("/", *cur_dir_split[1 : (idx + 1)])

        dbnd_system_file = os.path.join(cur_path, ".dbnd", "databand-system.cfg")
        if os.path.exists(dbnd_system_file):
            set_env_dir(ENV_DBND_HOME, cur_path)
            return True

        dbnd_home = __find_dbnd_home_at(cur_path)
        if dbnd_home:
            set_env_dir(ENV_DBND_HOME, dbnd_home)
            return True

    # last chance, we couldn't find dbnd project so we'll use user's home folder
    user_home = os.path.expanduser("~")
    if user_home:
        _debug_init_print("dbnd home was not found. Using user's home: %s" % user_home)
        set_env_dir(ENV_DBND_HOME, user_home)
        return True

    return False


def _env_banner():
    return "\tDBND_HOME={dbnd}\n\tDBND_SYSTEM={system}".format(
        dbnd=os.environ[ENV_DBND_HOME], system=os.environ[ENV_DBND_SYSTEM]
    )


_DBND_ENVIRONMENT = False


def _initialize_dbnd_home():
    global _DBND_ENVIRONMENT
    if _DBND_ENVIRONMENT:
        return
    _DBND_ENVIRONMENT = True

    _debug_init_print("_initialize_dbnd_home")
    project_config = get_dbnd_project_config()
    if project_config.disabled:
        _debug_init_print("databand is disabled via %s" % ENV_DBND__DISABLED)
        return

    _debug_init_print("Initializing Databand Basic Environment")
    _initialize_google_composer()
    _init_windows_python_path(_databand_package)

    # main logic
    __initialize_dbnd_home_environ()

    __initialize_airflow_home()

    if project_config.quiet_mode:
        # we should not print anything if we are in shell completion!
        import logging

        logging.getLogger().setLevel(logging.CRITICAL + 1)

    _debug_init_print(_env_banner())


def __initialize_dbnd_home_environ():
    # MAIN PART OF THE SCRIPT
    if ENV_DBND_HOME not in os.environ:
        _find_and_set_dbnd_home()

    if ENV_DBND_HOME not in os.environ:
        raise DatabandHomeError(
            "\nDBND_HOME could not be found when searching from current directory '%s' to root folder! \n "
            "Use `export DBND__DEBUG_INIT=True` to get more debug information.\n"
            "Trying fixing that issue by:\n"
            "\t 1. Explicitly set current directory to DBND HOME via: `export DBND_HOME=ROOT_OF_YOUR_PROJECT`.\n"
            "\t 2. `cd` into your project directory.\n"
            "\t 3. Create one of the following files inside current directory: [%s].\n"
            "\t 4. Run 'dbnd project-init' in current directory."
            % (os.getcwd(), ", ".join(_MARKER_FILES))
        )

    _dbnd_home = os.environ[ENV_DBND_HOME]

    if ENV_DBND_SYSTEM not in os.environ:
        dbnd_system = os.path.join(_dbnd_home, ".dbnd")

        # backward compatibility to $DBND_HOME/dbnd folder
        if not os.path.exists(dbnd_system) and os.path.exists(
            os.path.join(_dbnd_home, "dbnd")
        ):
            dbnd_system = os.path.join(_dbnd_home, "dbnd")

        os.environ[ENV_DBND_SYSTEM] = dbnd_system

    if ENV_DBND_LIB in os.environ:
        # usually will not happen
        _debug_init_print("Using DBND Library from %s" % os.environ[ENV_DBND_LIB])
    else:
        os.environ[ENV_DBND_LIB] = _databand_package


def __initialize_airflow_home():
    ENV_AIRFLOW_HOME = "AIRFLOW_HOME"

    if ENV_AIRFLOW_HOME in os.environ:
        # user settings - we do nothing
        _debug_init_print(
            "Found user defined AIRFLOW_HOME at %s" % os.environ[ENV_AIRFLOW_HOME]
        )
        return

    for dbnd_airflow_home in [
        os.path.join(os.environ[ENV_DBND_SYSTEM], "airflow"),
        os.path.join(os.environ[ENV_DBND_HOME], ".airflow"),
    ]:
        if not os.path.exists(dbnd_airflow_home):
            continue

        _debug_init_print(
            "Found airflow home folder at DBND, setting AIRFLOW_HOME to %s"
            % dbnd_airflow_home
        )
        os.environ[ENV_AIRFLOW_HOME] = dbnd_airflow_home


def _initialize_google_composer():
    if "COMPOSER_ENVIRONMENT" not in os.environ:
        return
    _debug_init_print("Initializing Google Composer Environment")

    if ENV_DBND_HOME not in os.environ:
        os.environ[ENV_DBND_HOME] = os.environ["HOME"]

    env_tracker_raise_on_error = "DBND__CORE__TRACKER_RAISE_ON_ERROR"
    if env_tracker_raise_on_error not in os.environ:
        os.environ[env_tracker_raise_on_error] = "false"


def dbnd_project_path(*path):
    return get_dbnd_project_config().dbnd_project_path(*path)
