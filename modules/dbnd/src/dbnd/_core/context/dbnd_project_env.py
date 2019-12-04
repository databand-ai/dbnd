#  << should be run before any import >>
# otherwise airflow.configuration will fail
# fix AIRFLOW_HOME for all runs

import os
import sys

from configparser import ConfigParser

from dbnd._vendor.snippets.airflow_configuration import expand_env_var


ENV_DBND_HOME = "DBND_HOME"
ENV_AIRFLOW_CONFIG = "AIRFLOW_CONFIG"
ENV_DBND_SYSTEM = "DBND_SYSTEM"
ENV_DBND_LIB = "DBND_LIB"
ENV_SHELL_COMPLETION = "_DBND_COMPLETE"

ENV_DBND__DEBUG_INIT = "DBND__DEBUG_INIT"
ENV_DBND__DISABLED_INIT = "DBND__DISABLED_INIT"

_DBND_DEBUG_INIT = bool(os.environ.get(ENV_DBND__DEBUG_INIT))
_SHELL_COMPLETION = ENV_SHELL_COMPLETION in os.environ


if _DBND_DEBUG_INIT:
    print("dbnd debug init process mode is on due to '%s'" % ENV_DBND__DEBUG_INIT)


def _debug_print(msg):
    if _DBND_DEBUG_INIT:
        print(msg)


def _abs_path_dirname(file, *path):
    return os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(file)), *path))


_databand_package = _abs_path_dirname(__file__, "..", "..")
_databand_config_location = _abs_path_dirname(
    __file__, "..", "..", "..", "dbnd"
)  # Temporary until full migration


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
        or " init_project" in cmdline
    )


def _set_env_dir(key, value, source="code"):
    key = key.upper()
    if key not in os.environ:
        os.environ[key] = os.path.abspath(value)
        _debug_print("%s: %s=%s" % (source, key, value))
        return True
    return False


def _process_cfg(config_path):
    found_dbnd_config = False
    try:
        parser = ConfigParser()
        parser.read(config_path)

        config_root, config_name = os.path.split(config_path)
        source = os.path.basename(config_path)
        if not parser.has_section("databand"):
            return False

        for config_key in ["dbnd_home", "dbnd_system", "dbnd_config"]:
            if not parser.has_option("databand", config_key):
                continue
            found_dbnd_config = True
            config_value = parser.get("databand", config_key)
            config_value = os.path.abspath(os.path.join(config_root, config_value))

            _set_env_dir(config_key, config_value, source=source)
    except Exception as ex:
        print("Failed to process %s: %s" % (config_path, ex))
    return found_dbnd_config


def _find_project_folder_marker():
    """
    check if we can have project marker file by import it
    """
    try:
        import _databand_project

        return _abs_path_dirname(_databand_project.__file__)
    except ImportError:
        _debug_print("Failed to find project marker")
    return None


def _check_folder_for_config(folder):
    for config_name, marker in [
        ("setup.cfg", False),
        ("tox.ini", False),
        ("databand.cfg", True),
        ("project.cfg", True),
    ]:
        config_path = os.path.abspath(os.path.join(folder, config_name))
        # print ("%s %s" % (cur_dir_split, config_path))
        if os.path.exists(config_path):
            _debug_print("Found %s" % config_path)
            if _process_cfg(config_path):
                _debug_print("Found config at %s" % config_path)
                return True
            if marker:
                _debug_print("Found marker file %s" % config_path)
                # if we have config file without any databand home --> let's assume that it's our root!
                _set_env_dir(
                    ENV_DBND_HOME, os.path.abspath(folder), "marker_" + config_name
                )
                return True
        else:
            _debug_print("Not found %s" % config_path)
    return False


def _set_project_root():
    # falling back to simple version
    if _is_init_mode():
        print(
            "Databand is using default location, please setup DBND_HOME or _databand.py"
        )
        _set_env_dir(ENV_DBND_HOME, os.path.abspath("."))
        return True

    project_folder = _find_project_folder_marker()
    if project_folder:
        _debug_print("Found project folder from marker %s" % project_folder)
        # we know about project folder, let try to find "custom" configs in it
        if not _check_folder_for_config(project_folder):
            _debug_print("No custom config! setting to marker")
            _set_env_dir(ENV_DBND_HOME, project_folder)
        return True

    # Let try to find databand.cfg by traversing current folder up to the root
    # any place that has databand.cfg - is the project!
    cur_dir = os.path.normpath(os.getcwd())
    cur_dir_split = cur_dir.split(os.sep)
    cur_dir_split_reversed = reversed(list(enumerate(cur_dir_split)))
    for idx, cur_folder in cur_dir_split_reversed:
        cur_path = os.path.join("/", *cur_dir_split[1 : (idx + 1)])
        if _check_folder_for_config(cur_path):
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
    if _SHELL_COMPLETION:
        # we should not print anything if we are in shell completion!
        import logging

        logging.getLogger("airflow").setLevel(logging.CRITICAL + 1)
        logging.getLogger("databand").setLevel(logging.CRITICAL + 1)

    # MAIN PART OF THE SCRIPT
    if ENV_DBND_HOME not in os.environ:
        _set_project_root()

    if ENV_DBND_HOME not in os.environ:
        raise DatabandHomeError(
            "Can't identify DBND_HOME! Current directory is '%s'\n "
            "Fix that by:"
            "\t 1. Explicitly specifying that via `export DBND_HOME=ROOT_OF_YOUR_PROJECT`\n"
            "\t 2. cd into your project directory\n"
            "\t 3. (very unlikely) have you run init on your project?" % (os.getcwd())
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
        os.environ[ENV_DBND_LIB] = _databand_config_location

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
            _databand_config_location, "conf", "airflow.cfg"
        )

    if _DBND_DEBUG_INIT:
        print(_env_banner())
    _init_windows_python_path()


# we run it automatically
# as we only affect environ variables
if ENV_DBND__DISABLED_INIT not in os.environ:
    DBND_IS_INITIALIZED = init_databand_env()
else:
    DBND_IS_INITIALIZED = False
