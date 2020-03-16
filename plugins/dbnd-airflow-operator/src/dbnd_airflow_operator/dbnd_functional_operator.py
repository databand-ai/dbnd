# PLEASE DO NOT MOVE/RENAME THIS FILE, IT'S SERIALIZED INTO AIRFLOW DB
import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from dbnd import PipelineTask, PythonTask
from dbnd._core.context.databand_context import DatabandContext
from dbnd._core.current import try_get_databand_run
from dbnd._core.run.databand_run import new_databand_run
from dbnd._core.task_build.task_context import TaskContextPhase
from dbnd._core.utils.json_utils import convert_to_safe_types
from targets import target


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
        dbnd_task_params_fields,
        **kwargs
    ):
        super(DbndFunctionalOperator, self).__init__(**kwargs)
        self._task_type = dbnd_task_type
        self.dbnd_task_id = dbnd_task_id

        self.dbnd_task_params_fields = dbnd_task_params_fields
        self.dbnd_xcom_inputs = dbnd_xcom_inputs
        self.dbnd_xcom_outputs = dbnd_xcom_outputs

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

    def execute(self, context):
        logger.debug("Running dbnd dbnd_task from airflow operator %s", self.task_id)

        # Airflow has updated all relevan fields in Operator definition with XCom values
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
            # rebuild task with new values
            with dbnd_task.ctrl.task_context(phase=TaskContextPhase.BUILD):
                dbnd_task = dbnd_task.clone(**new_kwargs)

            logger.info(
                dbnd_task.ctrl.banner(
                    "Running task '%s'." % dbnd_task.task_name, color="cyan"
                )
            )
            with dbnd_task.ctrl.task_context(phase=TaskContextPhase.RUN):
                needs_databand_run = not isinstance(
                    dbnd_task, (PipelineTask, PythonTask)
                )
                dr = try_get_databand_run()
                if needs_databand_run and not dr:
                    logger.info("Creating nplace databand run for driver dump")
                    with new_databand_run(context=dc, task_or_task_name=dbnd_task) as r:
                        r._init_without_run()
                        r.save_run()
                        dbnd_task._task_submit()
                else:
                    dbnd_task._task_submit()

        logger.debug("Finished to run %s", self)
        result = {
            output_name: convert_to_safe_types(getattr(dbnd_task, output_name))
            for output_name in self.dbnd_xcom_outputs
        }
        return result

    def on_kill(self):
        return self.get_dbnd_task().on_kill()
