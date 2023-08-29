# © Copyright Databand.ai, an IBM Company 2022

import logging
import os
import warnings

from dbnd._core.configuration.environ_config import (
    get_dbnd_project_config,
    is_unit_test_mode,
    should_fix_pyspark_imports,
)
from dbnd._core.utils.basics.signal_utils import register_graceful_sigterm
from dbnd._core.utils.platform.osx_compatible.requests_in_forked_process import (
    enable_osx_forked_request_calls,
)
from dbnd_run.airflow.airflow_env import _initialize_airflow_home


def _surpress_loggers():
    logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
    logging.getLogger("googleapiclient").setLevel(logging.WARN)


def _suppress_warnings():
    warnings.simplefilter("ignore", FutureWarning)


def fix_pyspark_imports():
    import sys

    pyspark_libs, regular_libs = [], []
    for p in sys.path:
        if "spark-core" in p or "pyspark.zip" in p:
            pyspark_libs.append(p)
        else:
            regular_libs.append(p)
    regular_libs.extend(pyspark_libs)
    sys.path = regular_libs


def dbnd_disable_databand_dags_loading(dbnd_config):
    """
    if we are running from "dbnd" entrypoint, we probably do not need to load Scheduled DAG
    this will prevent from every airflow command to access dbnd web api
    """

    if dbnd_config.getboolean("airflow", "auto_disable_scheduled_dags_load"):
        os.environ["DBND_DISABLE_SCHEDULED_DAGS_LOAD"] = "True"


def _dbnd_bootstrap_plugins():
    project_config = get_dbnd_project_config()

    if project_config.is_no_plugins:
        return

    from dbnd import dbnd_config
    from dbnd_run.plugin.dbnd_plugins import (
        pm,
        register_dbnd_plugins,
        register_dbnd_user_plugins,
    )

    if not project_config.disable_pluggy_entrypoint_loading:
        register_dbnd_plugins()

    user_plugins = dbnd_config.get("core", "plugins", None)
    if user_plugins:
        register_dbnd_user_plugins(user_plugins.split(","))

    if is_unit_test_mode():
        pm.hook.dbnd_setup_unittest()

    pm.hook.dbnd_setup_plugin()


_dbnd_bootstrap_orchestration_status = None


def dbnd_bootstrap_orchestration():
    """Runs dbnd bootstrapping."""
    global _dbnd_bootstrap_orchestration_status
    if _dbnd_bootstrap_orchestration_status is not None:
        return
    try:
        _dbnd_bootstrap_orchestration_status = "loading"
        from dbnd import dbnd_config

        _initialize_airflow_home()

        _dbnd_bootstrap_plugins()

        # now we can run user code ( at driver/task)
        from dbnd._core.configuration import environ_config
        from dbnd._core.utils.basics.load_python_module import run_user_func

        user_preinit = environ_config.get_user_preinit()
        if user_preinit:
            run_user_func(user_preinit)

        _surpress_loggers()
        _suppress_warnings()

        register_graceful_sigterm()

        if should_fix_pyspark_imports():
            fix_pyspark_imports()

        if dbnd_config.getboolean("run", "fix_env_on_osx"):
            enable_osx_forked_request_calls()

        _dbnd_bootstrap_orchestration_status = "loaded"
    except Exception as ex:
        _dbnd_bootstrap_orchestration_status = "error: %s" % str(ex)
        raise ex
