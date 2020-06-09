from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from dag_test_examples import t_A, t_B


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "dbnd_config": {"databand": {"env": "gcp"}},
}

with DAG(dag_id="dbnd_dag_at_gcp", default_args=default_args) as dag_remote_fs:
    a = t_A()
    b = t_B(a)

if __name__ == "__main__":
    dag_remote_fs.clear()
    dag_remote_fs.run(start_date=days_ago(0), end_date=days_ago(0))
