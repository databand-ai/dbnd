from airflow.utils.session import provide_session

from dbnd_airflow.contants import AIRFLOW_BELOW_2


if AIRFLOW_BELOW_2:
    from airflow.executors import LocalExecutor, SequentialExecutor
else:
    from airflow.executors.local_executor import LocalExecutor
    from airflow.executors.sequential_executor import SequentialExecutor
