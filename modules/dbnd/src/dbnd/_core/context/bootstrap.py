import logging
import os
import signal
import warnings

import dbnd

from dbnd._core.configuration.environ_config import (
    in_quiet_mode,
    is_sigquit_handler_on,
    is_unit_test_mode,
)
from dbnd._core.context.dbnd_project_env import _env_banner, init_databand_env
from dbnd._core.plugin.dbnd_plugins import is_airflow_enabled
from dbnd._core.plugin.dbnd_plugins_mng import (
    register_dbnd_plugins,
    register_dbnd_user_plugins,
)
from dbnd._core.utils.basics.signal_utils import safe_signal
from dbnd._core.utils.platform import windows_compatible_mode
from dbnd._core.utils.platform.osx_compatible.requests_in_forked_process import (
    enable_osx_forked_request_calls,
)


logger = logging.getLogger(__name__)


def _surpress_loggers():
    logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
    logging.getLogger("googleapiclient").setLevel(logging.WARN)


def _suppress_warnings():
    warnings.simplefilter("ignore", FutureWarning)


def _dbnd_exception_handling():
    if windows_compatible_mode:
        return

    # Enables graceful shutdown when running inside docker/kubernetes and subprocess shutdown
    # By default the kill signal is SIGTERM while our code mostly expects SIGINT (KeyboardInterrupt)
    def sigterm_handler(sig, frame):
        os.kill(os.getpid(), signal.SIGINT)

    safe_signal(signal.SIGTERM, sigterm_handler)


_dbnd_system_bootstrap = False


def dbnd_system_bootstrap():
    global _dbnd_system_bootstrap
    if _dbnd_system_bootstrap:
        return
    try:
        _dbnd_system_bootstrap = True
        # prevent recursive call, problematic on exception

        init_databand_env()

        if not in_quiet_mode():
            logger.info("Starting Databand %s!\n%s", dbnd.__version__, _env_banner())
        from databand import dbnd_config

        dbnd_config.load_system_configs()
    except Exception:
        _dbnd_system_bootstrap = False
        raise


_dbnd_bootstrap = False
_dbnd_bootstrap_started = False


def dbnd_bootstrap():
    global _dbnd_bootstrap
    global _dbnd_bootstrap_started
    if _dbnd_bootstrap_started:
        return
    _dbnd_bootstrap_started = True

    dbnd_system_bootstrap()
    from targets.marshalling import register_basic_data_marshallers

    register_basic_data_marshallers()

    _surpress_loggers()
    _suppress_warnings()
    enable_osx_forked_request_calls()

    if is_airflow_enabled():
        from dbnd_airflow.bootstrap import airflow_bootstrap

        airflow_bootstrap()

    register_dbnd_plugins()

    from dbnd._core.configuration import environ_config
    from dbnd._core.utils.basics.load_python_module import run_user_func
    from dbnd._core.plugin.dbnd_plugins import pm

    from dbnd._core.configuration.dbnd_config import config

    user_plugins = config.get("core", "plugins", None)
    if user_plugins:
        register_dbnd_user_plugins(user_plugins.split(","))

    if is_unit_test_mode():
        pm.hook.dbnd_setup_unittest()

    pm.hook.dbnd_setup_plugin()

    if is_sigquit_handler_on():
        from dbnd._core.utils.basics.signal_utils import (
            register_sigquit_stack_dump_handler,
        )

        register_sigquit_stack_dump_handler()

    # now we can run user code ( at driver/task)
    user_preinit = environ_config.get_user_preinit()
    if user_preinit:
        run_user_func(user_preinit)

    # if for any reason there will be code that calls dbnd_bootstrap, this will prevent endless recursion
    _dbnd_bootstrap = True
