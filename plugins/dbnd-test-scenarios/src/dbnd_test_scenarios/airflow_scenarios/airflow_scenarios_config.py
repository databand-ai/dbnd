import inspect

from airflow import DAG
from airflow.utils.dates import days_ago

from dbnd._core.inplace_run.airflow_utils import track_dag


DEFAULT_ARGS = {"owner": "staging", "start_date": days_ago(2)}


def dag_task_output(*path):
    return ("./output", *path)


def dag_task_output_partition_csv(name):
    return dag_task_output("%s.{{ds}}.csv", name)


def txcom(task_id):
    return "{{ti.xcom_pull(task_ids='%s')}}" % task_id


class SmartScheduler(object):
    frequent = "*/15 * * * *"
    daily = "* 1 * * *"


stg_schedule = SmartScheduler()


def magicDAG(dag_id, **kwargs):
    caller_globals = inspect.stack()[1][0].f_globals

    kwargs.setdefault("default_args", DEFAULT_ARGS)
    kwargs.setdefault("schedule_interval", SmartScheduler.daily)
    # catchup
    # tracking
    dag = TrackedDAG(dag_id=dag_id, **kwargs)

    # set dag at global space so it can be discovered
    caller_globals["DAG__%s" % dag_id] = dag
    return dag


class TrackedDAG(DAG):
    """
    Use only as `with TrackedDAG():`
    """

    def __exit__(self, _type, _value, _tb):
        super(TrackedDAG, self).__exit__(_type, _value, _tb)
        track_dag(self)
