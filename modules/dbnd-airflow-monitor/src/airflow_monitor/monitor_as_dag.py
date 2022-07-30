# © Copyright Databand.ai, an IBM Company 2022

import contextlib
import logging
import os
import threading
import time

from datetime import timedelta

import psutil

from airflow import settings
from airflow.exceptions import AirflowNotFoundException
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.version import version as airflow_version
from packaging import version as packaging_version


AIRFLOW_VERSION = packaging_version.parse(airflow_version)
if AIRFLOW_VERSION.major == 1 or (
    AIRFLOW_VERSION.major == 2 and AIRFLOW_VERSION.minor < 2
):
    TASK_CONCURRENCY_KEY = "task_concurrency"
else:  # airflow 2.2+
    TASK_CONCURRENCY_KEY = "max_active_tis_per_dag"

if AIRFLOW_VERSION.major == 1:
    from airflow.hooks.base_hook import BaseHook
    from airflow.operators.bash_operator import BashOperator
else:
    from airflow.hooks.base import BaseHook
    from airflow.operators.bash import BashOperator

# Do not change this name unless you change the same constant in constants.py in dbnd-airflow
MONITOR_DAG_NAME = "databand_airflow_monitor"

CHECK_INTERVAL = 10
AUTO_RESTART_TIMEOUT = 3 * 60 * 60
MEMORY_LIMIT = 8 * 1024 * 1024 * 1024

MEMORY_DIFF_BETWEEN_LOG_PRINTS_IN_MB = 5

FORCE_RESTART_TIMEOUT = timedelta(seconds=AUTO_RESTART_TIMEOUT + 5 * 60)
LOG_LEVEL = "INFO"
DATABAND_AIRFLOW_CONN_ID = "dbnd_config"

# This is the interval that we use to check that memory consumption does not cross the limit. Do NOT change it.
GUARD_SLEEP_INTERVAL_IN_SECONDS = 10

PRINT_MEMORY_CONSUMPTION_INTERVAL_IN_SECONDS = 60
ITERATION_PRINT_INTERVAL = (
    PRINT_MEMORY_CONSUMPTION_INTERVAL_IN_SECONDS / GUARD_SLEEP_INTERVAL_IN_SECONDS
)


logger = logging.getLogger(__name__)
args = {"owner": "Databand", "start_date": days_ago(2)}


class MonitorOperator(BashOperator):
    def __init__(
        self,
        databand_airflow_conn_id,
        log_level,
        custom_env=None,
        guard_memory=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.databand_airflow_conn_id = databand_airflow_conn_id
        self.log_level = log_level
        self.custom_env = custom_env
        self.guard_memory = guard_memory

    def execute(self, context):
        with start_guard_thread(self.guard_memory):
            if self.custom_env:
                self.env.update(self.custom_env)
            return super(MonitorOperator, self).execute(context)

    def pre_execute(self, context):
        self.env = os.environ.copy()

        try:
            dbnd_conn_config = BaseHook.get_connection(self.databand_airflow_conn_id)
            json_config = dbnd_conn_config.extra_dejson
            dbnd_config = self.to_env(self._flatten_dict(json_config, d_key="DBND"))
            self.env.update(dbnd_config)
        except AirflowNotFoundException:
            missing_env_variables = self._get_missing_env_variables()
            if missing_env_variables:
                raise Exception(
                    f"No Connection or {missing_env_variables} environment variables found, please set connection in Airflow or see https://docs.databand.ai/docs/access-token"
                )

        # AirflowMonitorConfig doesn't really have dag_ids config, so we avoid setting this environment variable
        # to avoid unnecessary warnings
        self.env.pop("DBND__AIRFLOW_MONITOR__DAG_IDS", None)
        self.env.update(
            {
                "DBND__LOG__LEVEL": self.log_level,
                "DBND__AIRFLOW_MONITOR__SQL_ALCHEMY_CONN": settings.SQL_ALCHEMY_CONN,
                "DBND__AIRFLOW_MONITOR__LOCAL_DAG_FOLDER": settings.DAGS_FOLDER,
                "DBND__AIRFLOW_MONITOR__FETCHER": "db",
                "DBND__LOG__DISABLE_COLORS": "TRUE",
                "DBND__LOG__FORMATTER_SIMPLE": "%(task)-5s - %(message)s",
            }
        )

    def _flatten_dict(self, d, d_key=""):
        """
        Flatten input dict to env variables:
        { "core": { "conf1": "v1", "conf2": "v2" } } =>
        { "dbnd__core__conf1": "v1", "dbnd__core__conf2": "v2" }
        """
        items = []
        sep = "__"

        for k, v in d.items():
            new_key = d_key + sep + k if d_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _get_missing_env_variables(self):
        expected_connection_env_variables = {
            "DBND__AIRFLOW_MONITOR__SYNCER_NAME",
            "DBND__CORE__DATABAND_ACCESS_TOKEN",
            "DBND__CORE__DATABAND_URL",
        }
        missing_connection_env_variables = expected_connection_env_variables.difference(
            os.environ
        )
        return missing_connection_env_variables

    def to_env(self, d):
        """
        convert dict to be env friendly - uppercase keys and stringify values
        """
        return {k.upper(): str(v) for k, v in d.items()}


def kill_processes(processes):
    logger.fatal("Memory usage went over limit, killing")
    for process in reversed(processes):
        try:
            logger.fatal("killing %s", process.pid)
            process.kill()
        except Exception:
            logger.fatal("Error while killing process %s", process.pid, exc_info=True)


def _check_memory_usage():
    current_process = psutil.Process(os.getpid())
    current_process_memory = current_process.memory_full_info()
    total_usage = current_process_memory.rss
    logger.debug(
        "Current process %s (%s) usage: %s",
        current_process.pid,
        current_process.name(),
        current_process_memory,
    )

    children = current_process.children(recursive=True)
    for child in children:
        child_memory = child.memory_full_info()
        total_usage += child_memory.rss
        logger.debug(
            "Child process %s (%s) usage: %s", child.pid, child.name(), child_memory
        )
    return children, total_usage


@contextlib.contextmanager
def start_guard_thread(memory_guard_limit, guard_sleep=GUARD_SLEEP_INTERVAL_IN_SECONDS):
    should_stop = False

    def memory_guard():
        logger.info(
            "Running memory guard with the limit=%s, checking every %s seconds",
            memory_guard_limit,
            guard_sleep,
        )
        current_usage_in_mb = 0
        iteration_number = 0
        while not should_stop:
            try:
                processes, total_usage = _check_memory_usage()
                total_usage_in_mb = int(total_usage / 1024 / 1024)
                if (
                    total_usage_in_mb
                    >= current_usage_in_mb + MEMORY_DIFF_BETWEEN_LOG_PRINTS_IN_MB
                ):
                    logger.info(
                        "Memory usage changed from: %s mb to %s mb",
                        current_usage_in_mb,
                        total_usage_in_mb,
                    )
                    current_usage_in_mb = total_usage_in_mb
                elif iteration_number % ITERATION_PRINT_INTERVAL == 0:
                    logger.info("Memory usage is %s mb", current_usage_in_mb)
                if memory_guard_limit and total_usage > memory_guard_limit:
                    kill_processes(processes)
                    return
                iteration_number += 1
            except Exception:
                logger.exception(
                    "Failed to run memory guard with limit=%s", memory_guard_limit
                )
                return
            time.sleep(guard_sleep)

    t = threading.Thread(target=memory_guard)
    try:
        t.start()
        yield
    finally:
        should_stop = True
        logger.info("Finalizing memory guard thread, waiting 15 seconds")
        t.join(timeout=15)


def get_monitor_dag(
    dag_id=MONITOR_DAG_NAME,
    check_interval=CHECK_INTERVAL,
    auto_restart_timeout=AUTO_RESTART_TIMEOUT,
    force_restart_timeout=FORCE_RESTART_TIMEOUT,
    databand_airflow_conn_id=DATABAND_AIRFLOW_CONN_ID,
    monitor_env=None,
    guard_memory=MEMORY_LIMIT,
    log_level=LOG_LEVEL,
):
    """
    @param dag_id: Name of Databand sync dag - default is "databand_airflow_monitor"
    @param check_interval: Sleep time (in seconds) between sync iterations
    @param auto_restart_timeout: Restart after this number of seconds
    @param force_restart_timeout: We're using FORCE_RESTART_TIMEOUT as backup mechanism for the case monitor is stuck for some reason.
    Normally it should auto-restart by itself after AUTO_RESTART_TIMEOUT, but in case it's not - we'd like to kill it.
    @param databand_airflow_conn_id: Name of databand connection in Airflow connections
    @param monitor_env: Custom Monitor Operator environment (use it to override DBND settings)
    @param guard_memory: Limit of memory used by monitor process (bytes, disabled if None)
    @param log_level: Dbnd log level
    """
    dag = DAG(
        dag_id=dag_id,
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
        operator_kwargs = {TASK_CONCURRENCY_KEY: 1}
        run_monitor = MonitorOperator(
            databand_airflow_conn_id=databand_airflow_conn_id,
            log_level=log_level,
            task_id="monitor",
            retries=10,
            bash_command="python3 -m dbnd airflow-monitor-v2 %s" % opts,
            retry_delay=timedelta(seconds=1),
            retry_exponential_backoff=False,
            max_retry_delay=timedelta(seconds=1),
            execution_timeout=force_restart_timeout,
            custom_env=monitor_env,
            guard_memory=guard_memory,
            **operator_kwargs,
        )

    return dag
