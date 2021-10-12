import logging
import os

from datetime import timedelta

from airflow import settings
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


CHECK_INTERVAL = 10
AUTO_RESTART_TIMEOUT = 30 * 60

FORCE_RESTART_TIMEOUT = timedelta(seconds=AUTO_RESTART_TIMEOUT + 5 * 60)
LOG_LEVEL = "WARN"
DATABAND_AIRFLOW_CONN_ID = "dbnd_config"

logger = logging.getLogger(__name__)

args = {
    "owner": "Databand",
    "start_date": days_ago(2),
}


class MonitorOperator(BashOperator):
    def __init__(self, databand_airflow_conn_id, log_level, **kwargs):
        super().__init__(**kwargs)
        self.databand_airflow_conn_id = databand_airflow_conn_id
        self.log_level = log_level

    def pre_execute(self, context):
        dbnd_conn_config = BaseHook.get_connection(self.databand_airflow_conn_id)
        json_config = dbnd_conn_config.extra_dejson

        dbnd_config = self.to_env(
            self.flatten(json_config, parent_key="DBND", sep="__")
        )

        self.env = os.environ.copy()
        self.env.update(dbnd_config)
        self.env.update(
            {
                "DBND__LOG__LEVEL": self.log_level,
                "DBND__AIRFLOW_MONITOR__SQL_ALCHEMY_CONN": settings.SQL_ALCHEMY_CONN,
                "DBND__AIRFLOW_MONITOR__LOCAL_DAG_FOLDER": settings.DAGS_FOLDER,
                "DBND__AIRFLOW_MONITOR__FETCHER": "db",
            }
        )

    def flatten(self, d, parent_key="", sep="_"):
        """
        Flatten input dict to env variables:
        { "core": { "conf1": "v1", "conf2": "v2" } } =>
        { "dbnd__core__conf1": "v1", "dbnd__core__conf2": "v2" }

        source: https://stackoverflow.com/a/6027615/15495440
        """
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def to_env(self, d):
        """
        convert dict to be env friendly - uppercase keys and stringify values
        """
        return {k.upper(): str(v) for k, v in d.items()}


def get_monitor_dag(
    check_interval=CHECK_INTERVAL,
    auto_restart_timeout=AUTO_RESTART_TIMEOUT,
    force_restart_timeout=FORCE_RESTART_TIMEOUT,
    databand_airflow_conn_id=DATABAND_AIRFLOW_CONN_ID,
    log_level=LOG_LEVEL,
):
    """
    @param check_interval: Sleep time (in seconds) between sync iterations
    @param auto_restart_timeout: Restart after this number of seconds
    @param force_restart_timeout: We're using FORCE_RESTART_TIMEOUT as backup mechanism for the case monitor is stuck for some reason.
    Normally it should auto-restart by itself after AUTO_RESTART_TIMEOUT, but in case it's not - we'd like to kill it.
    @param databand_airflow_conn_id: Name of databand connection in Airflow connections
    @param log_level: Dbnd log level
    """
    dag = DAG(
        dag_id="databand_airflow_monitor",
        default_args=args,
        schedule_interval="* * * * *",
        dagrun_timeout=None,
        max_active_runs=1,
        catchup=False,
    )
    if hasattr(dag, "tags"):
        dag.tags = ["project:airflow-monitor"]

    with dag:
        # show_env = BashOperator(task_id="env", bash_command="env")
        opts = " --interval %d " % check_interval
        if auto_restart_timeout:
            opts += " --stop-after %d " % auto_restart_timeout

        run_monitor = MonitorOperator(
            databand_airflow_conn_id=databand_airflow_conn_id,
            log_level=log_level,
            task_id="monitor",
            task_concurrency=1,
            retries=10,
            bash_command="python3 -m dbnd airflow-monitor-v2 %s" % opts,
            retry_delay=timedelta(seconds=1),
            retry_exponential_backoff=False,
            max_retry_delay=timedelta(seconds=1),
            execution_timeout=force_restart_timeout,
        )

    return dag
