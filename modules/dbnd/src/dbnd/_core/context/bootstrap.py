# © Copyright Databand.ai, an IBM Company 2022

import logging
import os
import traceback

import dbnd
import dbnd._core.context.use_dbnd_run

from dbnd._core.configuration.environ_config import get_dbnd_project_config
from dbnd._core.context.use_dbnd_run import set_orchestration_mode
from dbnd._core.log.dbnd_log import dbnd_log_debug, dbnd_log_info, is_verbose


logger = logging.getLogger(__name__)

_dbnd_bootstrap_status = None


def dbnd_bootstrap(enable_dbnd_run=False):
    """Runs dbnd bootstrapping."""
    global _dbnd_bootstrap_status
    dbnd_log_debug("Bootstrap entrypoint: %s" % _dbnd_bootstrap_status)
    if _dbnd_bootstrap_status is not None:
        return
    try:
        _dbnd_bootstrap_status = "loading"

        project_config = get_dbnd_project_config()

        if enable_dbnd_run:
            set_orchestration_mode()

        # DEBUGGING AND VERBOSE PRINTS
        if is_verbose():
            logger.info("Databand %s bootstrap!\n", dbnd.__version__)

        # in dev always print it if exists
        dbnd_run_info_source_version = os.environ.get("DBND__RUN_INFO__SOURCE_VERSION")
        if dbnd_run_info_source_version:
            dbnd_log_info("code revision: %s", dbnd_run_info_source_version)

        if project_config.is_sigquit_handler_on:
            from dbnd._core.utils.basics.signal_utils import (
                register_sigquit_stack_dump_handler,
            )

            register_sigquit_stack_dump_handler()

        from dbnd import dbnd_config

        dbnd_config.load_system_configs()

        if dbnd._core.context.use_dbnd_run.is_orchestration_mode():
            from dbnd_run.orchestration_bootstrap import dbnd_bootstrap_orchestration

            dbnd_bootstrap_orchestration()
        else:
            _dbnd_bootstrap_tracking()

        _dbnd_bootstrap_status = "loaded"
    except Exception as ex:
        _dbnd_bootstrap_status = "bootstrap error: %s %s" % (
            traceback.format_exc(),
            traceback.format_stack(),
        )
        dbnd_log_debug("Failed to bootstrap dbnd %s" % ex)
        raise ex


def _dbnd_bootstrap_tracking():
    from dbnd.providers.spark.dbnd_spark_init import try_load_spark_env

    try_load_spark_env()
