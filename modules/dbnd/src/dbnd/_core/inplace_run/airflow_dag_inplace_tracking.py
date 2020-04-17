import datetime
import logging
import os

from typing import Any, Optional

import pytz

import attr

from dbnd._core.configuration import environ_config
from dbnd._core.constants import UpdateSource
from dbnd._core.context.databand_context import DatabandContext, new_dbnd_context
from dbnd._core.decorator.dynamic_tasks import (
    create_dynamic_task,
    run_dynamic_task_safe,
)
from dbnd._core.decorator.func_task_call import FuncCall
from dbnd._core.inplace_run.tracking_config import set_tracking_config_overide
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.run.databand_run import new_databand_run
from dbnd._core.task.task import Task
from dbnd._core.task_build.task_context import (
    TaskContextPhase,
    current_phase,
    current_task,
    has_current_task,
)
from dbnd._core.task_run.task_run import TaskRunUidGen
from dbnd._core.utils.airflow_cmd_utils import generate_airflow_cmd
from dbnd._core.utils.basics.memoized import cached
from dbnd._core.utils.seven import import_errors
from dbnd._core.utils.string_utils import task_name_for_runtime
from dbnd._core.utils.uid_utils import get_job_run_uid, get_task_run_uid
from dbnd._vendor import pendulum


logger = logging.getLogger(__name__)
_SPARK_ENV_FLAG = "SPARK_ENV_LOADED"  # if set, we are in spark

DAG_SPECIAL_TASK_ID = "DAG"


def override_airflow_log_system_for_tracking():
    return environ_config.environ_enabled(
        environ_config.ENV_DBND__OVERRIDE_AIRFLOW_LOG_SYSTEM_FOR_TRACKING
    )


def disable_airflow_subdag_tracking():
    return environ_config.environ_enabled(
        environ_config.ENV_DBND__DISABLE_AIRFLOW_SUBDAG_TRACKING, False
    )


@attr.s
class AirflowTaskContext(object):
    dag_id = attr.ib()  # type: str
    execution_date = attr.ib()  # type: str
    task_id = attr.ib()  # type: str

    root_dag_id = attr.ib(init=False, repr=False)  # type: str
    is_subdag = attr.ib(init=False, repr=False)  # type: bool

    def __attrs_post_init__(self):
        self.is_subdag = "." in self.dag_id and not disable_airflow_subdag_tracking()
        if self.is_subdag:
            dag_breadcrumb = self.dag_id.split(".", 1)
            self.root_dag_id = dag_breadcrumb[0]
            self.parent_dags = []
            # for subdag "root_dag.subdag1.subdag2.subdag3" it should return
            # root_dag, root_dag.subdag1, root_dag.subdag1.subdag2
            # WITHOUT the dag_id itself!
            cur_name = []
            for name_part in dag_breadcrumb[:-1]:
                cur_name.append(name_part)
                self.parent_dags.append(".".join(cur_name))
        else:
            self.root_dag_id = self.dag_id
            self.parent_dags = []


_AIRFLOW_TASK_CONTEXT = None
_TRY_GET_AIRFLOW_CONTEXT_CACHE = {}


@cached(cache=_TRY_GET_AIRFLOW_CONTEXT_CACHE)
def try_get_airflow_context():
    # type: ()-> Optional[AirflowTaskContext]
    # first try to get from spark
    try:
        from_spark = try_get_airflow_context_from_spark_conf()
        if from_spark:
            return from_spark

        dag_id = os.environ.get("AIRFLOW_CTX_DAG_ID")
        execution_date = os.environ.get("AIRFLOW_CTX_EXECUTION_DATE")
        task_id = os.environ.get("AIRFLOW_CTX_TASK_ID")
        if dag_id and task_id and execution_date:
            return AirflowTaskContext(
                dag_id=dag_id, execution_date=execution_date, task_id=task_id
            )
        return None
    except Exception:
        return None


_IS_SPARK_INSTALLED = None


def _is_dbnd_spark_installed():
    global _IS_SPARK_INSTALLED
    if _IS_SPARK_INSTALLED is not None:
        return _IS_SPARK_INSTALLED
    try:
        try:
            from pyspark import SparkContext

            from dbnd_spark import dbnd_spark_bootstrap

            dbnd_spark_bootstrap()
        except import_errors:
            _IS_SPARK_INSTALLED = False
        else:
            _IS_SPARK_INSTALLED = True
    except Exception:
        # safeguard, on any exception
        _IS_SPARK_INSTALLED = False

    return _IS_SPARK_INSTALLED


def try_get_airflow_context_from_spark_conf():
    if (
        not environ_config.environ_enabled("DBND__ENABLE__SPARK_CONTEXT_ENV")
        or _SPARK_ENV_FLAG not in os.environ
    ):
        return None

    if not _is_dbnd_spark_installed():
        return None
    try:
        from pyspark import SparkContext

        conf = SparkContext.getOrCreate().getConf()

        dag_id = conf.get("spark.env.AIRFLOW_CTX_DAG_ID")
        execution_date = conf.get("spark.env.AIRFLOW_CTX_EXECUTION_DATE")
        task_id = conf.get("spark.env.AIRFLOW_CTX_TASK_ID")

        if dag_id and task_id and execution_date:
            return AirflowTaskContext(
                dag_id=dag_id, execution_date=execution_date, task_id=task_id
            )
    except Exception as ex:
        logger.info("Failed to get airlfow context info from spark job: %s", ex)

    return None


class _AirflowRuntimeTask(Task):
    task_is_system = True
    _conf__track_source_code = False

    dag_id = parameter[str]
    execution_date = parameter[datetime.datetime]

    is_dag = parameter(system=True).value(False)

    def _initialize(self):
        super(_AirflowRuntimeTask, self)._initialize()
        self.ctrl.force_task_run_uid = TaskRunUidGen_TaskAfId(self.dag_id)
        self.task_meta.task_functional_call = ""

        self.task_meta.task_command_line = generate_airflow_cmd(
            dag_id=self.dag_id,
            task_id=self.task_id,
            execution_date=self.execution_date,
            is_root_task=self.is_dag,
        )


class AirflowDagRuntimeTask(_AirflowRuntimeTask):
    pass


class AirflowOperatorRuntimeTask(_AirflowRuntimeTask):
    pass


# there can be only one tracking manager
_CURRENT_AIRFLOW_TRACKING_MANAGER = None  # type: Optional[AirflowTrackingManager]


def track_airflow_dag_run_operator_run(func_call, airflow_task_context):
    atm = get_airflow_tracking_manager(airflow_task_context)
    if not atm:
        # we failed to get tracker
        return func_call.invoke()

    # @task runs inside this function
    return atm.run_airflow_dynamic_task(func_call)


def get_airflow_tracking_manager(airflow_task_context):
    global _CURRENT_AIRFLOW_TRACKING_MANAGER
    if _CURRENT_AIRFLOW_TRACKING_MANAGER is False:
        # we already failed to create ourself once
        return None

    try:
        # this part will run DAG and Operator Tasks
        if _CURRENT_AIRFLOW_TRACKING_MANAGER is None:
            # this is our first call!
            _CURRENT_AIRFLOW_TRACKING_MANAGER = AirflowTrackingManager(
                af_context=airflow_task_context
            )
        return _CURRENT_AIRFLOW_TRACKING_MANAGER
    except Exception:
        _CURRENT_AIRFLOW_TRACKING_MANAGER = False
        logger.warning("Error during dbnd pre-init, ignoring", exc_info=True)
        return None


class AirflowTrackingManager(object):
    def __init__(self, af_context):
        # type: (AirflowTaskContext) -> None
        self.run_uid = get_job_run_uid(
            dag_id=af_context.root_dag_id, execution_date=af_context.execution_date
        )
        self.dag_id = af_context.dag_id
        # this is the real operator uid, we need to connect to it with our "tracked" task,
        # so the moment monitor is on -> we can sync
        af_runtime_op_task_id = af_context.task_id
        self.af_operator_sync__task_run_uid = get_task_run_uid(
            self.run_uid, af_context.dag_id, af_runtime_op_task_id
        )
        # 1. create proper DatabandContext so we can create other objects
        set_tracking_config_overide(
            use_dbnd_log=override_airflow_log_system_for_tracking()
        )

        # create databand context
        with new_dbnd_context(name="airflow") as dc:  # type: DatabandContext

            # now create "operator" task for current task_id,
            # we can't actually run it, we even don't know when it's going to finish
            # current execution is inside the operator, this is the only thing we know
            # STATE AFTER INIT:
            # AirflowOperator__runtime ->  DAG__runtime
            task_target_date = pendulum.parse(
                af_context.execution_date, tz=pytz.UTC
            ).date()
            # AIRFLOW OPERATOR RUNTIME

            af_runtime_op = AirflowOperatorRuntimeTask(
                task_family=task_name_for_runtime(af_runtime_op_task_id),
                dag_id=af_context.dag_id,
                execution_date=af_context.execution_date,
                task_target_date=task_target_date,
                task_version="%s:%s"
                % (af_runtime_op_task_id, af_context.execution_date),
            )

            # this is the real operator uid, we need to connect to it with our "tracked" task,
            # so the moment monitor is on -> we can sync
            af_db_op_task_run_uid = get_task_run_uid(
                self.run_uid, af_context.dag_id, af_runtime_op_task_id
            )
            af_runtime_op.task_meta.extra_parents_task_run_uids.add(
                af_db_op_task_run_uid
            )
            af_runtime_op.ctrl.force_task_run_uid = TaskRunUidGen_TaskAfId(
                af_context.dag_id
            )

            self.af_operator_runtime__task = af_runtime_op
            # AIRFLOW DAG RUNTIME
            self.af_dag_runtime__task = AirflowDagRuntimeTask(
                task_name=task_name_for_runtime(DAG_SPECIAL_TASK_ID),
                dag_id=af_context.root_dag_id,  # <- ROOT DAG!
                execution_date=af_context.execution_date,
                task_target_date=task_target_date,
            )
            _add_child(self.af_dag_runtime__task, self.af_operator_runtime__task)

            # this will create databand run with driver and root tasks.
            # we need the "root" task to be the same between different airflow tasks invocations
            # since in dbnd we must have single root task, so we create "dummy" task with dag_id name

            # create databand run
            # we will want to preserve
            with new_databand_run(
                context=dc,
                task_or_task_name=self.af_dag_runtime__task,
                run_uid=self.run_uid,
                existing_run=False,
                job_name=af_context.root_dag_id,
                send_heartbeat=False,  # we don't send heartbeat in tracking
                source=UpdateSource.airflow_tracking,
            ) as dr:
                self.dr = dr
                dr._init_without_run()
                self.airflow_operator__task_run = dr.get_task_run_by_id(
                    af_runtime_op.task_id
                )

    def run_airflow_dynamic_task(self, func_call):
        # type: (FuncCall) -> Any
        if has_current_task():
            can_run_nested = False
            try:
                current = current_task()
                phase = current_phase()
                if (
                    phase is TaskContextPhase.RUN
                    and current.settings.dynamic_task.enabled
                    and current.task_supports_dynamic_tasks
                ):
                    can_run_nested = True
            except Exception:
                return _handle_tracking_error(func_call, "nested-check")

            if can_run_nested:
                return self._create_and_run_dynamic_task_safe(
                    func_call, attach_to_monitor_op=False
                )
            else:
                # unsupported mode
                return func_call.invoke()

        context_enter_ok = False
        try:
            with self.dr.run_context():
                with self.airflow_operator__task_run.runner.task_run_execution_context():
                    context_enter_ok = True
                    return self._create_and_run_dynamic_task_safe(
                        func_call, attach_to_monitor_op=True
                    )
        except Exception:
            if context_enter_ok:
                raise
            return _handle_tracking_error(func_call, "context-enter")

    def _create_and_run_dynamic_task_safe(self, func_call, attach_to_monitor_op):
        try:
            task = create_dynamic_task(func_call)
            if attach_to_monitor_op:
                # attach task to the Operator created by Monitor
                task.task_meta.extra_parents_task_run_uids.add(
                    self.af_operator_sync__task_run_uid
                )
        except Exception:
            return _handle_tracking_error(func_call, "task-create")

        # we want all tasks to be "consistent" between all retries
        # so we will see airflow retries as same task_run
        task.ctrl.force_task_run_uid = TaskRunUidGen_TaskAfId_Runtime(self.dag_id)
        return run_dynamic_task_safe(task=task, func_call=func_call)


def _handle_tracking_error(func_call, msg):
    logger.warning(
        "Failed during dbnd %s, ignoring, and continue without tracking" % msg,
        exc_info=True,
    )
    global _CURRENT_AIRFLOW_TRACKING_MANAGER
    _CURRENT_AIRFLOW_TRACKING_MANAGER = False
    return func_call.invoke()


def _add_child(parent, child):
    parent.set_upstream(child)
    parent.task_meta.add_child(child.task_id)


class TaskRunUidGen_TaskAfId(TaskRunUidGen):
    def __init__(self, dag_id):
        super(TaskRunUidGen_TaskAfId, self).__init__()
        self.dag_id = dag_id

    def generate_task_run_uid(self, run, task, task_af_id):
        return get_task_run_uid(run.run_uid, self.dag_id, task_af_id)


class TaskRunUidGen_TaskAfId_Runtime(TaskRunUidGen):
    def __init__(self, dag_id):
        super(TaskRunUidGen_TaskAfId_Runtime, self).__init__()
        self.dag_id = dag_id

    def generate_task_run_uid(self, run, task, task_af_id):
        runtime_af = (
            _CURRENT_AIRFLOW_TRACKING_MANAGER.airflow_operator__task_run.task_af_id
        )
        return get_task_run_uid(
            run.run_uid, self.dag_id, "%s.%s" % (runtime_af, task_af_id)
        )
