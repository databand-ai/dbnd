import airflow

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


args = {"start_date": airflow.utils.dates.days_ago(2), "owner": "royi"}
dag = DAG(dag_id="plugin_test_dag_3", default_args=args, schedule_interval=None)


def task1():
    print("Hello World!")
    return "Hello World!"


def task2():
    print("Shalom Olam!")
    return "Shalom Olam!"


def task3():
    print("Ola Mundo!")
    return "Ola Mundo!"


op1 = PythonOperator(task_id="task1", python_callable=task1, dag=dag,)
op2 = PythonOperator(task_id="task2", python_callable=task2, dag=dag,)
op3 = PythonOperator(task_id="task3", python_callable=task3, dag=dag,)

op1 >> op2 >> op3
