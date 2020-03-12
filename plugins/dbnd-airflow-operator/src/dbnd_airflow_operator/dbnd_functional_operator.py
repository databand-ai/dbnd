# PLEASE DO NOT MOVE/RENAME THIS FILE, IT'S SERIALIZED INTO AIRFLOW DB
import logging

from typing import List

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from more_itertools import unique_everseen

from dbnd._core.context.databand_context import DatabandContext
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
        template_fields = kwargs.pop("template_fields", None)
        super(DbndFunctionalOperator, self).__init__(**kwargs)
        self._task_type = dbnd_task_type
        self.dbnd_task_id = dbnd_task_id

        self.dbnd_task_params_fields = dbnd_task_params_fields
        self.dbnd_xcom_inputs = dbnd_xcom_inputs
        self.dbnd_xcom_outputs = dbnd_xcom_outputs

        # make a copy
        all_template_fields = list(self.template_fields)  # type: List[str]
        if template_fields:
            all_template_fields.extend(template_fields)
        all_template_fields.extend(self.dbnd_task_params_fields)

        self.__class__.template_fields = list(unique_everseen(all_template_fields))
        # self.template_fields = self.__class__.template_fields

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
        logger.debug("Running dbnd task from airflow operator %s", self.task_id)

        # Airflow has updated all relevan fields in Operator definition with XCom values
        # now we can create a real dbnd task with real references to task
        new_kwargs = {}
        for p_name in self.dbnd_task_params_fields:
            new_kwargs[p_name] = getattr(self, p_name, None)
            # this is the real input value after
            if p_name in self.dbnd_xcom_inputs:
                new_kwargs[p_name] = target(new_kwargs[p_name])

        dag_ctrl = self.get_dbnd_dag_ctrl()
        with DatabandContext.context(_context=dag_ctrl.dbnd_context) as dc:
            logger.debug("Running %s with kwargs=%s ", self.task_id, new_kwargs)
            dbnd_task = dc.task_instance_cache.get_task_by_id(self.dbnd_task_id)
            with dbnd_task.ctrl.task_context(phase=TaskContextPhase.BUILD):
                task = dbnd_task.clone(**new_kwargs)

            with dbnd_task.ctrl.task_context(phase=TaskContextPhase.RUN):
                task._task_submit()

        logger.debug("Finished to run %s", self)
        result = {
            output_name: convert_to_safe_types(getattr(task, output_name))
            for output_name in self.dbnd_xcom_outputs
        }
        return result

    def on_kill(self):
        return self.get_dbnd_task().on_kill()
