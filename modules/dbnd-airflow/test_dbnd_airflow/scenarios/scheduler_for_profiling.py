import logging
import os


# before we load any config!
os.environ["DBND_DISABLE_SCHEDULED_DAGS_LOAD"] = "True"
os.environ["DBND__LOG__SQLALCHEMY_PROFILE"] = "True"
os.environ["DBND__LOG__SQLALCHEMY_TRACE"] = "True"
print(os.environ["DBND__LOG__SQLALCHEMY_TRACE"])


# in order to profile scheduler.py Dag Processor you need to add folowing lines to scheduler_job.py:145
# # ========
# # DBND
# from dbnd_airflow.airflow_extensions.airflow_config import reinit_airflow_sql_conn
# reinit_airflow_sql_conn()
#
# from dbnd._core.log.logging_utils import create_file_handler
# from test_dbnd_airflow.scenarios.scheduler_perf_experiment import log_processor_file
#
# logging.getLogger("airflow.processor").addHandler(create_file_handler(log_file=log_processor_file))
# # ========


def main():
    try:
        from airflow import conf
    except ImportError:
        from airflow.configuration import conf

    from airflow.jobs.scheduler_job import SchedulerJob
    from airflow.models import DagBag

    from dbnd import dbnd_bootstrap
    from dbnd._core.log.logging_utils import create_file_handler
    from dbnd_airflow.executors.simple_executor import InProcessExecutor
    from test_dbnd_airflow.scenarios.scheduler_perf_experiment import (
        dag_folder,
        dag_id,
        log_scheduler,
    )

    dbnd_bootstrap()
    conf.set("core", "unit_test_mode", "True")

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


if __name__ == "__main__":
    main()
