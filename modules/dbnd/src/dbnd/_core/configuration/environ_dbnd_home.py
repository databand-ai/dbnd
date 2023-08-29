# © Copyright Databand.ai, an IBM Company 2022

import os

from configparser import ConfigParser

from dbnd._core.configuration.environ_config import (
    ENV_DBND_HOME,
    ENV_DBND_LIB,
    ENV_DBND_SYSTEM,
)
from dbnd._core.configuration.project_env import _is_init_mode
from dbnd._core.errors import DatabandError
from dbnd._core.log import dbnd_log_init_msg
from dbnd._core.utils.basics.environ_utils import set_env_dir
from dbnd._core.utils.basics.path_utils import abs_join, relative_path


_databand_package = relative_path(__file__, "..", "..")

_MARKER_FILES = ["databand.cfg", "project.cfg", "databand-system.cfg"]


class DatabandHomeError(DatabandError):
    pass


def _find_project_by_import():
    """
    check if we can have project marker file by import it
    """
    try:
        import _databand_project

        return abs_join(_databand_project.__file__, "..")
    except ImportError:
        dbnd_log_init_msg("Can't import `_databand_project` marker.")
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
                dbnd_log_init_msg("%s: %s=%s" % (source, config_key, config_value))

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
        dbnd_log_init_msg(
            "Found dbnd home by at %s, by config file: %s" % (dbnd_home, config_file)
        )
        return dbnd_home

    dbnd_home, marker_file = _has_marker_file(folder)
    if dbnd_home:
        dbnd_log_init_msg(
            "Found dbnd home by at %s, by marker file: %s" % (dbnd_home, marker_file)
        )
        return dbnd_home

    dbnd_log_init_msg("dbnd home was not found at %s" % folder)
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
        dbnd_log_init_msg(
            "Found project folder by import from marker %s" % project_folder
        )
        # we know about project folder, let try to find "custom" configs in it
        dbnd_home = __find_dbnd_home_at(project_folder)
        if not dbnd_home:
            dbnd_log_init_msg(
                "No markers at %s! setting dbnd_home to %s" % (dbnd_home, dbnd_home)
            )
            dbnd_home = project_folder

        set_env_dir(ENV_DBND_HOME, dbnd_home)
        return True

    # traversing all the way up to until finding relevant anchor files
    cur_dir = os.path.normpath(os.getcwd())
    cur_dir_split = cur_dir.split(os.sep)
    cur_dir_split_reversed = reversed(list(enumerate(cur_dir_split)))
    dbnd_log_init_msg(
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
        dbnd_log_init_msg("dbnd home was not found. Using user's home: %s" % user_home)
        set_env_dir(ENV_DBND_HOME, user_home)
        return True

    return False


def _env_banner():
    return "\tDBND_HOME={dbnd}\n\tDBND_SYSTEM={system}".format(
        dbnd=os.environ[ENV_DBND_HOME], system=os.environ[ENV_DBND_SYSTEM]
    )


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
        dbnd_log_init_msg("Using DBND Library from %s" % os.environ[ENV_DBND_LIB])
    else:
        os.environ[ENV_DBND_LIB] = _databand_package

    dbnd_log_init_msg(_env_banner())
