from datetime import timedelta

from airflow.utils.dates import days_ago


default_args_dbnd_scenarios_dag = {
    "owner": "test_scenarios",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
