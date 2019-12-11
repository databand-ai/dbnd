import logging
import os
import typing

import dbnd

from dbnd_airflow.utils import AIRFLOW_LEGACY_URL_KEY


if typing.TYPE_CHECKING:
    from typing import Optional

    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.task_run.task_run import TaskRun


@dbnd.hookimpl
def dbnd_setup_plugin():
    # Set additional airflow configuration
    configure_sql_alchemy_conn()

    from dbnd import register_config_cls
    from dbnd_airflow.config import AirflowFeaturesConfig

    register_config_cls(AirflowFeaturesConfig)


@dbnd.hookimpl
def dbnd_get_commands():
    from dbnd_airflow.cli.cmd_airflow import run_task_airflow, airflow
    from dbnd_airflow.cli.cmd_airflow_webserver import airflow_webserver
    from dbnd_airflow.cli.cmd_airflow_db import (
        airflow_db_init,
        airflow_db_reset,
        airflow_db_upgrade,
    )
    from dbnd_airflow.cli.cmd_scheduler import scheduler

    return [
        airflow,
        airflow_webserver,
        airflow_db_init,
        airflow_db_reset,
        airflow_db_upgrade,
        run_task_airflow,
        scheduler,
    ]


@dbnd.hookimpl
def dbnd_on_exit_context(ctx):
    if ctx._airflow_op_catcher_dag:
        ctx._airflow_op_catcher_dag.__exit__(None, None, None)


@dbnd.hookimpl
def dbnd_post_enter_context(ctx):  # type: (DatabandContext) -> None
    from dbnd._core.utils.platform import windows_compatible_mode
    from dbnd_airflow.dbnd_task_executor.airflow_operators_catcher import (
        DatabandOpCatcherDag,
    )

    # doing this causes infinite loops when trying to log from inside an airflow task
    if ctx.name != "airflow":
        airflow_task_log = logging.getLogger("airflow.task")
        # we will move airflow.task file handler to upper level
        # on task run all stdout/stderr will be redirected to root logger
        # all other messages will get to it automatically.
        if airflow_task_log.handlers and not windows_compatible_mode:
            airflow_task_log_handler = airflow_task_log.handlers[0]
            logging.root.handlers.append(airflow_task_log_handler)

        airflow_task_log.propagate = True
        airflow_task_log.handlers = []

    from dbnd_airflow.config import get_dbnd_default_args

    ctx._airflow_op_catcher_dag = DatabandOpCatcherDag(
        dag_id="inline_airflow_ops", default_args=get_dbnd_default_args()
    )
    ctx._airflow_op_catcher_dag.__enter__()


def configure_sql_alchemy_conn():
    from dbnd_airflow.airflow_extensions.airflow_config import (
        set_airflow_sql_conn_from_dbnd_config,
    )

    set_airflow_sql_conn_from_dbnd_config()


@dbnd.hookimpl
def dbnd_setup_unittest():
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    from airflow import configuration as airflow_configuration
    from airflow.configuration import TEST_CONFIG_FILE

    # we can't call load_test_config, as it override airflow.cfg
    # we want to keep it as base
    logging.info("Reading Airflow test config at %s" % TEST_CONFIG_FILE)
    airflow_configuration.conf.read(TEST_CONFIG_FILE)


@dbnd.hookimpl
def dbnd_task_run_context(task_run):
    # type: (TaskRun)-> Optional[Context]
    """ Using this context when running task_run  """
    if hasattr(task_run.task.ctrl, "airflow_op") and task_run.is_root:
        links_dict = {
            AIRFLOW_LEGACY_URL_KEY: "{{airflow_base_url}}/task?dag_id={}&execution_date={}&task_id={}".format(
                task_run.run.job_name, task_run.run.execution_date, task_run.task_af_id
            )
        }
        task_run.set_external_resource_urls(links_dict)
