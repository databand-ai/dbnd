# © Copyright Databand.ai, an IBM Company 2022

import logging
import os

import dbnd

from dbnd._core.configuration.environ_config import (
    _env_banner,
    get_dbnd_project_config,
    is_unit_test_mode,
)


logger = logging.getLogger(__name__)

_dbnd_system_bootstrap = None


def dbnd_bootstrap(dbnd_entrypoint=False):
    """Runs dbnd bootstrapping."""
    global _dbnd_system_bootstrap
    if _dbnd_system_bootstrap is not None:
        return
    try:
        _dbnd_system_bootstrap = "loading"

        # this will also initialize env if it's not initialized
        project_config = get_dbnd_project_config()
        if not project_config.quiet_mode:
            logger.info("Starting Databand %s!\n%s", dbnd.__version__, _env_banner())
            dbnd_run_info_source_version = os.environ.get(
                "DBND__RUN_INFO__SOURCE_VERSION"
            )
            if dbnd_run_info_source_version:
                logger.info("revision: %s", dbnd_run_info_source_version)
        from dbnd import dbnd_config

        dbnd_config.load_system_configs()

        from dbnd.providers.spark.dbnd_spark_init import try_load_spark_env

        try_load_spark_env()

        if project_config.is_sigquit_handler_on:
            from dbnd._core.utils.basics.signal_utils import (
                register_sigquit_stack_dump_handler,
            )

            register_sigquit_stack_dump_handler()

        _dbnd_bootstrap_plugins_and_entrypoints()

        # MOVE
        from dbnd.orchestration.orchestration_bootstrap import (
            dbnd_bootstrap_orchestration,
        )

        dbnd_bootstrap_orchestration(dbnd_entrypoint=dbnd_entrypoint)

        _dbnd_system_bootstrap = "loaded"
    except Exception as ex:
        _dbnd_system_bootstrap = "error: %s" % str(ex)
        raise ex


def _dbnd_bootstrap_plugins_and_entrypoints():
    from dbnd._core.configuration import environ_config
    from dbnd._core.configuration.dbnd_config import config
    from dbnd._core.utils.basics.load_python_module import run_user_func
    from dbnd.orchestration.plugin.dbnd_plugins import (
        pm,
        register_dbnd_plugins,
        register_dbnd_user_plugins,
    )

    if config.getboolean("core", "dbnd_plugins_enabled"):
        register_dbnd_plugins()

        user_plugins = config.get("core", "plugins", None)
        if user_plugins:
            register_dbnd_user_plugins(user_plugins.split(","))

        if is_unit_test_mode():
            pm.hook.dbnd_setup_unittest()

        pm.hook.dbnd_setup_plugin()

    # now we can run user code ( at driver/task)
    user_preinit = environ_config.get_user_preinit()
    if user_preinit:
        run_user_func(user_preinit)
