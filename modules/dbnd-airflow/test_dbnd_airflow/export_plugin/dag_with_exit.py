import airflow

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


args = {"start_date": airflow.utils.dates.days_ago(2), "owner": "royi"}
dag = DAG(dag_id="dag_with_exit", default_args=args, schedule_interval=None)


def task1():
    print("Hello World!")
    return "Hello World!"


op1 = PythonOperator(task_id="task1", python_callable=task1, dag=dag,)

op1
exit(2)
