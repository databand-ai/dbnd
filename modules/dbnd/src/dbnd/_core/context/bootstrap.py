import logging
import os
import signal
import sys
import threading

import dbnd

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.configuration.environ_config import (
    is_shell_cmd_complete_mode,
    set_dbnd_unit_test_mode,
)
from dbnd._core.context.dbnd_project_env import (
    ENV_DBND_HOME,
    _env_banner,
    init_databand_env,
)
from dbnd._core.plugin.dbnd_plugins import (
    is_airflow_enabled,
    register_dbnd_plugins,
    register_dbnd_user_plugins,
)
from dbnd._core.utils.platform import windows_compatible_mode


logger = logging.getLogger(__name__)


def _surpress_loggers():
    logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
    logging.getLogger("googleapiclient").setLevel(logging.WARN)


# override exception hooks
def excepthook(exctype, value, traceback):
    if exctype == KeyboardInterrupt:
        sys.exit(1)
    else:
        sys.__excepthook__(exctype, value, traceback)


def _dbnd_exception_handling():
    import six

    if six.PY3:
        sys.excepthook = excepthook

    if windows_compatible_mode:
        return

    # Enables graceful shutdown when running inside docker/kubernetes and subprocess shutdown
    # (used by Airflow for watchers).
    # By default the kill signal is SIGTERM while our code mostly expects SIGINT (KeyboardInterrupt)
    try:
        if isinstance(threading.current_thread(), threading._MainThread):
            signal.signal(
                signal.SIGTERM, lambda sig, frame: os.kill(os.getpid(), signal.SIGINT)
            )
    except Exception as ex:
        pass


_dbnd_bootstrap = False


def dbnd_bootstrap(unittest=False):

    global _dbnd_bootstrap
    if _dbnd_bootstrap:
        return
    _dbnd_bootstrap = True

    if ENV_DBND_HOME not in os.environ:
        init_databand_env()

    if not is_shell_cmd_complete_mode():
        logger.info("Starting Databand %s!\n%s", dbnd.__version__, _env_banner())
    _dbnd_exception_handling()

    if unittest:
        set_dbnd_unit_test_mode()

    _surpress_loggers()

    config.load_system_configs()

    if is_airflow_enabled():
        from dbnd_airflow.bootstrap import airflow_bootstrap

        airflow_bootstrap()

    register_dbnd_plugins()

    from dbnd._core.configuration import environ_config
    from dbnd._core.utils.basics.load_python_module import run_user_func
    from dbnd._core.plugin.dbnd_plugins import pm

    user_plugins = config.get("core", "plugins", None)
    if user_plugins:
        register_dbnd_user_plugins(user_plugins.split(","))

    if unittest:
        pm.hook.dbnd_setup_unittest()

    pm.hook.dbnd_setup_plugin()

    # now we can run user code ( at driver/task)
    user_preinit = environ_config.get_user_preinit()
    if user_preinit:
        run_user_func(user_preinit)
