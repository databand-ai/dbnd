import logging
import os

from airflow import conf
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.models import DagBag

from dbnd import dbnd_bootstrap
from dbnd._core.log.logging_utils import create_file_handler
from dbnd_airflow.airflow_extensions.airflow_config import reinit_airflow_sql_conn
from dbnd_airflow.executors.simple_executor import InProcessExecutor
from test_dbnd_airflow.scenarios.scheduler_perf_experiment import (
    dag_folder,
    dag_id,
    log_scheduler,
)


# before we load any config!
os.environ["DBND_DISABLE_SCHEDULED_DAGS_LOAD"] = "True"
os.environ["DBND__LOG__SQLALCHEMY_PROFILE"] = "True"
os.environ["DBND__LOG__SQLALCHEMY_TRACE"] = "True"


dbnd_bootstrap()


reinit_airflow_sql_conn()

conf.set("core", "unit_test_mode", "True")
logger = logging.getLogger(__name__)

logging.root.addHandler(create_file_handler(log_file=log_scheduler))

dag_bag = DagBag(dag_folder=dag_folder)
scheduler_job = SchedulerJob(
    dag_ids=[dag_id],
    subdir=dag_folder,
    do_pickle=False,
    num_runs=3,
    executor=InProcessExecutor(dag_bag=dag_bag),
)

scheduler_job.run()
