import logging
import os
import typing

import dbnd


AIRFLOW_LEGACY_URL_KEY = "airflow"
logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from typing import Optional

    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.task_run.task_run import TaskRun


@dbnd.hookimpl
def dbnd_setup_plugin():
    # Set additional airflow configuration
    configure_airflow_sql_alchemy_conn()

    from dbnd import register_config_cls
    from dbnd_airflow.config import AirflowConfig

    register_config_cls(AirflowConfig)


@dbnd.hookimpl
def dbnd_on_exit_context(ctx):
    if ctx._airflow_op_catcher_dag:
        ctx._airflow_op_catcher_dag.__exit__(None, None, None)


@dbnd.hookimpl
def dbnd_post_enter_context(ctx):  # type: (DatabandContext) -> None
    from dbnd_airflow.dbnd_task_executor.airflow_operators_catcher import (
        DatabandOpCatcherDag,
    )

    from dbnd_airflow.config import get_dbnd_default_args

    ctx._airflow_op_catcher_dag = DatabandOpCatcherDag(
        dag_id="inline_airflow_ops", default_args=get_dbnd_default_args()
    )
    ctx._airflow_op_catcher_dag.__enter__()


def configure_airflow_sql_alchemy_conn():
    from dbnd_airflow.airflow_extensions.airflow_config import reinit_airflow_sql_conn

    reinit_airflow_sql_conn()


@dbnd.hookimpl
def dbnd_setup_unittest():
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    from airflow import configuration as airflow_configuration
    from airflow.configuration import TEST_CONFIG_FILE

    # we can't call load_test_config, as it override airflow.cfg
    # we want to keep it as base
    logger.info("Reading Airflow test config at %s" % TEST_CONFIG_FILE)
    airflow_configuration.conf.read(TEST_CONFIG_FILE)

    sql_alchemy_conn = airflow_configuration.get("core", "sql_alchemy_conn")
    if sql_alchemy_conn.find("unittests.db") == -1:
        logger.warning(
            "You should set SQL_ALCHEMY_CONN to sqlite:///.../unittests.db for tests! %s"
            % sql_alchemy_conn
        )
    from dbnd_airflow.airflow_extensions.airflow_config import reinit_airflow_sql_conn

    reinit_airflow_sql_conn()

    from dbnd_airflow.dbnd_airflow_main import subprocess_airflow

    subprocess_airflow(args=["initdb"])
