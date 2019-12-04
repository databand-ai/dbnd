import logging
import typing

from dbnd._core.current import get_databand_run
from dbnd_airflow.dbnd_task_executor.airflow_operator_as_dbnd import (
    AirflowOperatorAsDbndTask,
)
from dbnd_airflow_contrib.dbnd_operator import DbndOperator


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun


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

    # this is the Airflow native Operator
    # we will want to call it with Databand wrapper
    # we are at the airflow operator that is part of databand dag
    dbnd_task_run = get_databand_run().get_task_run_by_id(dbnd_task_id)
    if isinstance(dbnd_task_run.task, AirflowOperatorAsDbndTask):
        # we need to update it with latest, as we have "templated" and copy airflow operator object
        dbnd_task_run.task.airflow_op = airflow_operator
        return dbnd_task_run.runner.execute(context)
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
    return _dbnd_operator_to_taskrun(dbnd_operator).runner.execute(
        airflow_context=context
    )


def dbnd_operator__kill(dbnd_operator):
    return _dbnd_operator_to_taskrun(dbnd_operator).task.on_kill()
