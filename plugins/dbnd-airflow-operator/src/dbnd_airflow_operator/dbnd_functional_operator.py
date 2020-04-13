# PLEASE DO NOT MOVE/RENAME THIS FILE, IT'S SERIALIZED INTO AIRFLOW DB
import logging
import typing

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from dbnd import Task, dbnd_handle_errors
from dbnd._core.context.databand_context import DatabandContext
from dbnd._core.run.databand_run import new_databand_run
from dbnd._core.task_build.task_context import TaskContextPhase
from dbnd._core.utils.json_utils import convert_to_safe_types
from dbnd._core.utils.uid_utils import get_job_run_uid
from targets import target


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun

logger = logging.getLogger(__name__)


class DbndFunctionalOperator(BaseOperator):
    """
    This is the Airflow operator that is created for every Databand Task


    it assume all tasks inputs coming from other airlfow tasks are in the format

    """

    ui_color = "#ffefeb"

    @apply_defaults
    def __init__(
        self,
        dbnd_task_type,
        dbnd_task_id,
        dbnd_xcom_inputs,
        dbnd_xcom_outputs,
        dbnd_overridden_output_params,
        dbnd_task_params_fields,
        **kwargs
    ):
        super(DbndFunctionalOperator, self).__init__(**kwargs)
        self._task_type = dbnd_task_type
        self.dbnd_task_id = dbnd_task_id

        self.dbnd_task_params_fields = dbnd_task_params_fields
        self.dbnd_xcom_inputs = dbnd_xcom_inputs
        self.dbnd_xcom_outputs = dbnd_xcom_outputs
        self.dbnd_overridden_output_params = dbnd_overridden_output_params

    @property
    def task_type(self):
        return "task"

    # we should not use @properties, it affects pickling of the object (?!)
    def get_dbnd_dag_ctrl(self):
        dag = self.dag

        from dbnd_airflow_operator.dbnd_functional_dag import DagFuncOperatorCtrl

        return DagFuncOperatorCtrl.build_or_get_dag_ctrl(dag)

    def get_dbnd_task(self):
        return self.get_dbnd_dag_ctrl().dbnd_context.task_instance_cache.get_task_by_id(
            self.dbnd_task_id
        )

    @dbnd_handle_errors(exit_on_error=False)
    def execute(self, context):
        logger.debug("Running dbnd dbnd_task from airflow operator %s", self.task_id)

        dag = context["dag"]
        execution_date = context["execution_date"]
        dag_id = dag.dag_id
        run_uid = get_job_run_uid(dag_id=dag_id, execution_date=execution_date)

        # Airflow has updated all relevant fields in Operator definition with XCom values
        # now we can create a real dbnd dbnd_task with real references to dbnd_task
        new_kwargs = {}
        for p_name in self.dbnd_task_params_fields:
            new_kwargs[p_name] = getattr(self, p_name, None)
            # this is the real input value after
            if p_name in self.dbnd_xcom_inputs:
                new_kwargs[p_name] = target(new_kwargs[p_name])

        new_kwargs["_dbnd_disable_airflow_inplace"] = True
        dag_ctrl = self.get_dbnd_dag_ctrl()
        with DatabandContext.context(_context=dag_ctrl.dbnd_context) as dc:
            logger.debug("Running %s with kwargs=%s ", self.task_id, new_kwargs)
            dbnd_task = dc.task_instance_cache.get_task_by_id(self.dbnd_task_id)
            # rebuild task with new values coming from xcom->operator
            with dbnd_task.ctrl.task_context(phase=TaskContextPhase.BUILD):
                dbnd_task = dbnd_task.clone(
                    output_params_to_clone=self.dbnd_overridden_output_params,
                    **new_kwargs
                )

            logger.debug("Creating inplace databand run for driver dump")
            dag_task = Task(task_name=dag.dag_id, task_target_date=execution_date)
            dag_task.set_upstream(dbnd_task)

            # create databand run
            with new_databand_run(
                context=dc,
                task_or_task_name=dag_task,
                run_uid=run_uid,
                existing_run=False,
                job_name=dag.dag_id,
            ) as dr:  # type: DatabandRun
                dr._init_without_run()

                # dr.driver_task_run.set_task_run_state(state=TaskRunState.RUNNING)
                # "make dag run"
                # dr.root_task_run.set_task_run_state(state=TaskRunState.RUNNING)
                dbnd_task_run = dr.get_task_run_by_id(dbnd_task.task_id)

                needs_databand_run_save = dbnd_task._conf__require_run_dump_file
                if needs_databand_run_save:
                    dr.save_run()

                logger.info(
                    dbnd_task.ctrl.banner(
                        "Running task '%s'." % dbnd_task.task_name, color="cyan"
                    )
                )
                # should be replaced with  tr._execute call
                dbnd_task_run.runner.execute(
                    airflow_context=context, handle_sigterm=False
                )

            logger.debug("Finished to run %s", self)
            result = {
                output_name: convert_to_safe_types(getattr(dbnd_task, output_name))
                for output_name in self.dbnd_xcom_outputs
            }
        return result

    def on_kill(self):
        return self.get_dbnd_task().on_kill()
