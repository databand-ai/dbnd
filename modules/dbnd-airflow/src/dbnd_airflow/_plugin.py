import logging
import os
import typing

import dbnd


AIRFLOW_LEGACY_URL_KEY = "airflow"
logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd._core.context.databand_context import DatabandContext


_airflow_op_catcher_dag = None


@dbnd.hookimpl
def dbnd_setup_plugin():
    # Set additional airflow configuration
    configure_airflow_sql_alchemy_conn()

    from dbnd import register_config_cls
    from dbnd_airflow.config import AirflowConfig

    register_config_cls(AirflowConfig)


@dbnd.hookimpl
def dbnd_on_exit_context(ctx):
    global _airflow_op_catcher_dag
    if _airflow_op_catcher_dag:
        _airflow_op_catcher_dag.__exit__(None, None, None)
        _airflow_op_catcher_dag = None


@dbnd.hookimpl
def dbnd_post_enter_context(ctx):  # type: (DatabandContext) -> None
    from dbnd_airflow.dbnd_task_executor.airflow_operators_catcher import (
        DatabandOpCatcherDag,
    )
    from dbnd_airflow.config import get_dbnd_default_args

    global _airflow_op_catcher_dag

    import airflow

    if airflow.settings.CONTEXT_MANAGER_DAG:
        # we are inside native airflow DAG or already have DatabandOpCatcherDag
        return
    _airflow_op_catcher_dag = DatabandOpCatcherDag(
        dag_id="inline_airflow_ops", default_args=get_dbnd_default_args()
    )
    _airflow_op_catcher_dag.__enter__()


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

    from dbnd_airflow.bootstrap import set_airflow_sql_conn_from_dbnd_config

    set_airflow_sql_conn_from_dbnd_config()

    # init db first
    from dbnd_airflow.dbnd_airflow_main import subprocess_airflow_initdb

    subprocess_airflow_initdb()

    # now reconnnect
    from dbnd_airflow.airflow_extensions.airflow_config import reinit_airflow_sql_conn

    reinit_airflow_sql_conn()
