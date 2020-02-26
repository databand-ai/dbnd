#  << should be run before any import >>
# otherwise airflow.configuration will fail
# fix AIRFLOW_HOME for all runs

import os
import sys

from configparser import ConfigParser

from dbnd._core.configuration.environ_config import (
    ENV_AIRFLOW_CONFIG,
    ENV_DBND_LIB,
    in_quiet_mode,
)
from dbnd._vendor.snippets.airflow_configuration import expand_env_var


ENV_DBND_HOME = "DBND_HOME"
ENV_DBND_SYSTEM = "DBND_SYSTEM"

ENV_DBND__DEBUG_INIT = "DBND__DEBUG_INIT"
ENV_DBND__DISABLED_INIT = "DBND__DISABLED_INIT"

_DBND_DEBUG_INIT = bool(os.environ.get(ENV_DBND__DEBUG_INIT))

_MARKER_FILES = ["databand.cfg", "project.cfg", "databand-system.cfg"]

if _DBND_DEBUG_INIT:
    print("dbnd debug init process mode is on due to '%s'" % ENV_DBND__DEBUG_INIT)


def _debug_print(msg):
    if _DBND_DEBUG_INIT:
        print(msg)


def _abs_path_dirname(file, *path):
    return os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(file)), *path))


_databand_package = _abs_path_dirname(__file__, "..", "..")


class DatabandHomeError(Exception):
    pass


def _is_init_mode():
    from subprocess import list2cmdline

    cmdline = list2cmdline(sys.argv)
    return (
        "dbnd project-init" in cmdline
        or cmdline == "-c project-init"
        or " project-init" in cmdline
        # backward compatibility
        or "dbnd init_project" in cmdline
        or cmdline == "-c init_project"
    )


def _set_env_dir(key, value, source="code"):
    key = key.upper()
    if key not in os.environ:
        os.environ[key] = os.path.abspath(value)
        _debug_print("%s: %s=%s" % (source, key, value))
        return True
    return False


def _find_project_by_import():
    """
    check if we can have project marker file by import it
    """
    try:
        import _databand_project

        return _abs_path_dirname(_databand_project.__file__)
    except ImportError:
        _debug_print("Failed to find project marker")
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

            if parser.has_option("databand", "dbnd_home"):
                dbnd_home = parser.get("databand", "dbnd_home")
                found_dbnd_home = os.path.abspath(os.path.join(config_root, dbnd_home))
                config_file = config_root

            for config_key in ["dbnd_system", "dbnd_config"]:
                # TODO: hidden magic, do we need these setters?
                if not parser.has_option("databand", config_key):
                    continue
                config_value = parser.get("databand", config_key)
                config_value = os.path.abspath(os.path.join(config_root, config_value))
                _set_env_dir(config_key, config_value, source=source)

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


def _get_dbnd_home(folder):
    dbnd_home, config_file = _process_cfg(folder)
    if dbnd_home:
        _debug_print(
            "Found dbnd home by at %s, by config file: %s" % (dbnd_home, config_file)
        )
        return dbnd_home

    dbnd_home, marker_file = _has_marker_file(folder)
    if dbnd_home:
        _debug_print(
            "Found dbnd home by at %s, by marker file: %s" % (dbnd_home, marker_file)
        )
        return dbnd_home

    _debug_print("dbnd home was not found at %s" % folder)
    return False


def _set_project_root():
    # falling back to simple version
    if _is_init_mode():
        print(
            "Databand is using default location, please setup DBND_HOME or _databand.py"
        )
        _set_env_dir(ENV_DBND_HOME, os.path.abspath("."))
        return True

    project_folder = _find_project_by_import()
    if project_folder:
        _debug_print("Found project folder by import from marker %s" % project_folder)
        # we know about project folder, let try to find "custom" configs in it
        dbnd_home = _get_dbnd_home(project_folder)
        if not dbnd_home:
            _debug_print("No custom config! setting current folder")
            dbnd_home = project_folder

        _set_env_dir(ENV_DBND_HOME, dbnd_home)
        return True

    _debug_print("Trying to find dbnd_home by traversing up to the root folder")
    cur_dir = os.path.normpath(os.getcwd())
    cur_dir_split = cur_dir.split(os.sep)
    cur_dir_split_reversed = reversed(list(enumerate(cur_dir_split)))

    for idx, cur_folder in cur_dir_split_reversed:
        cur_path = os.path.join("/", *cur_dir_split[1 : (idx + 1)])

        dbnd_system_file = os.path.join(cur_path, ".dbnd", "databand-system.cfg")
        if os.path.exists(dbnd_system_file):
            _set_env_dir(ENV_DBND_HOME, cur_path)
            return True

        dbnd_home = _get_dbnd_home(cur_path)
        if dbnd_home:
            _set_env_dir(ENV_DBND_HOME, dbnd_home)
            return True

    return False


def _env_banner():
    return "\tDBND_HOME={dbnd}\n\tDBND_SYSTEM={system}".format(
        dbnd=os.environ[ENV_DBND_HOME], system=os.environ[ENV_DBND_SYSTEM]
    )


def _init_windows_python_path():
    # patch pwd and resource system modules
    if os.name == "nt":
        sys.path.insert(
            0,
            os.path.join(_databand_package, "utils", "platform", "windows_compatible"),
        )


def init_databand_env():
    if in_quiet_mode():
        # we should not print anything if we are in shell completion!
        import logging

        logging.getLogger().setLevel(logging.CRITICAL + 1)

    # MAIN PART OF THE SCRIPT
    if ENV_DBND_HOME not in os.environ:
        _set_project_root()

    if ENV_DBND_HOME not in os.environ:
        raise DatabandHomeError(
            "\nDBND_HOME could not be found when searching from current directory '%s' to root folder!\n "
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
        if not os.path.exists(dbnd_system) and os.path.exists(
            os.path.join(_dbnd_home, "dbnd")
        ):
            dbnd_system = os.path.join(_dbnd_home, "dbnd")
        os.environ[ENV_DBND_SYSTEM] = dbnd_system

    if ENV_DBND_LIB not in os.environ:
        os.environ[ENV_DBND_LIB] = _databand_package

    if "AIRFLOW_HOME" not in os.environ:
        os.environ["AIRFLOW_HOME"] = airflow_home = os.path.join(
            os.environ[ENV_DBND_SYSTEM], "airflow"
        )
    else:
        airflow_home = expand_env_var(os.environ["AIRFLOW_HOME"])

    # airflow config override
    airflow_user_config = os.path.join(airflow_home, "airflow.cfg")
    if ENV_AIRFLOW_CONFIG not in os.environ and not os.path.exists(airflow_user_config):
        # User did not define his own airflow configuration
        # Use dbnd core lib configuration
        os.environ[ENV_AIRFLOW_CONFIG] = os.path.join(
            _databand_package, "conf", "airflow.cfg"
        )

    if _DBND_DEBUG_INIT:
        print(_env_banner())
    _init_windows_python_path()


# is it in use yet?
def _is_running_airflow_webserver():
    # TODO: ...
    if not sys.argv or len(sys.argv) < 2:
        return False

    if sys.argv[0].endswith("airflow") and sys.argv[1] == "webserver":
        return True

    if sys.argv[0].endswith("gunicorn") and "airflow-webserver" in sys.argv:
        return True

    return False


def _detect_composer():
    if "COMPOSER_ENVIRONMENT" in os.environ:
        if ENV_DBND_HOME not in os.environ:
            os.environ[ENV_DBND_HOME] = os.environ["HOME"]

        env_tracker_raise_on_error = "DBND__CORE__TRACKER_RAISE_ON_ERROR"
        if env_tracker_raise_on_error not in os.environ:
            os.environ[env_tracker_raise_on_error] = "false"


# we run it automatically
# as we only affect environ variables
if ENV_DBND__DISABLED_INIT not in os.environ:
    _detect_composer()
    DBND_IS_INITIALIZED = init_databand_env()
else:
    DBND_IS_INITIALIZED = False
