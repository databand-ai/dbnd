# © Copyright Databand.ai, an IBM Company 2022

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": datetime(year=2020, month=1, day=13),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

_FIRST_LEVEL_TASKS = 10
_SECOND_LEVEL_TASKS = 10

dag = DAG(
    "big_dag_%s_%s" % (_FIRST_LEVEL_TASKS, _SECOND_LEVEL_TASKS),
    default_args=default_args,
    description="A simple big dag",
    catchup=True,
    schedule_interval=timedelta(minutes=1),
)


def print_context(ds, ti, **kwargs):
    print("Running %s %s", ti.task_id, ti.execution_date)
    return "Whatever you return gets printed in the logs"


def generate_parallel_tasks(name_prefix, num_of_tasks, deps):
    tasks = []
    for t_id in range(num_of_tasks):
        run_this = PythonOperator(
            task_id="%s_%s" % (name_prefix, t_id),
            provide_context=True,
            python_callable=print_context,
            dag=dag,
        )
        run_this.set_upstream(deps)
        tasks.append(run_this)
    return tasks


zero_level_tasks = generate_parallel_tasks("l0", 1, [])
first_level_tasks = generate_parallel_tasks("l1", _FIRST_LEVEL_TASKS, zero_level_tasks)
second_level_tasks = generate_parallel_tasks(
    "l2", _SECOND_LEVEL_TASKS, first_level_tasks
)
