# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow.models import DAG, BaseOperator

from dbnd import PipelineTask, Task, output, parameter


logger = logging.getLogger(__name__)


def bind_task_to_op(task, airflow_operator):
    task.ctrl.airflow_op = airflow_operator


class AirflowOperatorAsDbndTask(Task):
    """
    Every Airflow Native Operator will be wrapped with AirflowTask
    """

    airflow_task_id = parameter[str]

    task_target_date = None

    airflow_op = parameter(significant=False)[BaseOperator]
    dag = parameter(significant=False)[DAG]

    original_execute = None
    _completed_airflow = False

    def _complete(self):
        return self._completed_airflow

    @classmethod
    def build_airflow_task(cls, airflow_operator, dag):
        task = cls(
            task_name=airflow_operator.task_id,
            airflow_task_id=airflow_operator.task_id,
            airflow_op=airflow_operator,
            dag=dag,
        )
        task.ctrl.airflow_op = airflow_operator
        airflow_operator.dbnd_task_id = task.task_id
        return task

    def run(self):
        af_task = self.airflow_op

        # TODO: use mechanism for "system" metrics
        for attr in af_task.__class__.template_fields:
            content = getattr(af_task, attr)
            if content and attr != "env":
                self.log_metric("airflow__%s" % attr, content)

        airflow_context = self.current_task_run.airflow_context
        assert airflow_context
        result = self.airflow_op.execute(airflow_context)
        self._completed_airflow = True
        return result


class AirflowDagAsDbndTask(PipelineTask):
    dag_id = parameter[str]

    # `get_runnable_airflow_dag` check if the task is AirflowDagTask
    # if it's AirflowDagTask  - we will use original DAG
    dag = parameter(significant=False)[DAG]
    roots = output

    def band(self):
        tasks = {}
        for airflow_op in self.dag.tasks:
            task = AirflowOperatorAsDbndTask.build_airflow_task(
                airflow_operator=airflow_op, dag=self.dag
            )
            tasks[airflow_op.task_id] = task

        for task in tasks.values():
            task.set_upstream(
                [tasks[op.task_id] for op in task.airflow_op.upstream_list]
            )

        self.roots = [tasks[r.task_id] for r in self.dag.roots]

    @classmethod
    def build_dbnd_task_from_dag(cls, dag):
        return cls(task_name=dag.dag_id, dag=dag, dag_id=dag.dag_id)
