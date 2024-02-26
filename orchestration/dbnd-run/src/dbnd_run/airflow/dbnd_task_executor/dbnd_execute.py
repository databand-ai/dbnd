# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import logging
import sys
import typing

from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd_run.airflow.dbnd_airflow_contrib.dbnd_operator import DbndOperator
from dbnd_run.airflow.dbnd_task_executor.airflow_operator_as_dbnd import (
    AirflowOperatorAsDbndTask,
)
from dbnd_run.run_executor.run_executor import RunExecutor, set_active_run_context
from dbnd_run.task_ctrl.task_run_executor import TaskRunExecutor


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun

logger = logging.getLogger(__name__)


def dbnd_execute_airflow_operator(airflow_operator, context):
    """
    Airflow Operator execute function
    """
    dbnd_task_id = getattr(airflow_operator, "dbnd_task_id", None)
    if not dbnd_task_id:
        return airflow_operator.execute(context)

    # operator is wrapped/created by databand
    if isinstance(airflow_operator, DbndOperator):
        return airflow_operator.execute(context)

    from dbnd._core.current import get_databand_run

    # this is the Airflow native Operator
    # we will want to call it with Databand wrapper
    # we are at the airflow operator that is part of databand dag
    dbnd_task_run = get_databand_run().get_task_run_by_id(dbnd_task_id)
    if isinstance(dbnd_task_run.task, AirflowOperatorAsDbndTask):
        # we need to update it with latest, as we have "templated" and copy airflow operator object
        dbnd_task_run.task.airflow_op = airflow_operator
        return dbnd_task_run.task_run_executor.execute(context)
    else:
        logging.info(
            "Found airflow operator with dbnd_task_id that can not be run by dbnd: %s",
            airflow_operator,
        )
        return airflow_operator.execute(context)


# wrappers for DbndOperator


def _dbnd_operator_to_taskrun(operator):
    # type: (DbndOperator)-> TaskRun
    from dbnd._core.current import get_databand_run

    return get_databand_run().get_task_run_by_id(operator.dbnd_task_id)


def dbnd_operator__execute(dbnd_operator, context):
    from dbnd._core.current import try_get_databand_run
    from targets import target

    run = try_get_databand_run()
    if not run:
        # we are not inside dbnd run, probably we are running from native airflow
        # let's try to load it:
        try:

            executor_config = dbnd_operator.executor_config
            logger.info("context: %s", context)

            logger.info("task.executor_config: %s", dbnd_operator.executor_config)
            logger.info("ti.executor_config: %s", context["ti"].executor_config)
            driver_dump = executor_config["DatabandExecutor"].get("dbnd_driver_dump")
            print(
                "Running dbnd task %s from %s"
                % (dbnd_operator.dbnd_task_id, driver_dump),
                file=sys.__stderr__,
            )

            if executor_config["DatabandExecutor"].get(
                "remove_airflow_std_redirect", False
            ):
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__

            dbnd_bootstrap(enable_dbnd_run=True)
            run_executor = RunExecutor.load_run(
                dump_file=target(driver_dump), disable_tracking_api=False
            )
            run = run_executor.run
        except Exception as e:
            print(
                "Failed to load dbnd task in native airflow execution! Exception: %s"
                % (e,),
                file=sys.__stderr__,
            )
            raise

        with set_active_run_context(run):
            task_run = run.get_task_run_by_id(dbnd_operator.dbnd_task_id)

            task_run.airflow_context = context
            # In the case the airflow_context has a different try_number than our task_run_executor's attempt_number,
            # we need to update our task_run_executor attempt accordingly.
            if task_run.attempt_number != context["ti"].try_number:
                task_run.set_task_run_attempt(context["ti"].try_number)
                task_run.task_run_executor = TaskRunExecutor(
                    task_run,
                    run_executor=task_run.task_run_executor.run_executor,
                    task_engine=task_run.task_run_executor.task_engine,
                )

            ret_value = task_run.task_run_executor.execute(airflow_context=context)
    else:
        task_run = run.get_task_run_by_id(dbnd_operator.dbnd_task_id)
        ret_value = task_run.task_run_executor.execute(airflow_context=context)

    return ret_value


def dbnd_operator__kill(dbnd_operator):
    from dbnd._core.current import try_get_databand_run

    run = try_get_databand_run()
    if not run:
        return

    task_run = run.get_task_run_by_id(dbnd_operator.dbnd_task_id)
    return task_run.task.on_kill()


def dbnd_operator__get_task_retry_delay(dbnd_operator):
    """
    This method overrides the task retry delay found in airflow.
    We must override the actual task retry delay from airflow to ensure that we can control the retry delay
    per task, for example when we send pods to retry, we may want a different delay rather than another engine
    """
    from dbnd._core.current import try_get_databand_run

    run = try_get_databand_run()
    if not run:
        logging.info(
            "Retry delay=%s, we are running from native Airflow process",
            dbnd_operator._retry_delay,
        )
        return dbnd_operator._retry_delay

    task_run = run.get_task_run_by_id(dbnd_operator.dbnd_task_id)

    if (
        task_run.task_run_executor.task_engine.task_definition.task_family
        == "kubernetes"
    ):
        # If we are running in K8s - use pod retry delay instead of task retry delay

        return task_run.task_run_executor.task_engine.pod_default_retry_delay
    else:
        return task_run.task.task_retry_delay
