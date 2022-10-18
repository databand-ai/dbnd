# © Copyright Databand.ai, an IBM Company 2022

from datetime import datetime, timedelta

from dbnd import parameter
from dbnd._core.task import Config
from dbnd._core.utils.platform.windows_compatible.getuser import dbnd_getuser


class AirflowConfig(Config):
    """Apache Airflow (currently a part of Databand)"""

    _conf__task_family = "airflow"

    enable_dbnd_context_vars = parameter(
        default=True,
        description="Enable extended airflow context variables, including dbnd information.",
    )[bool]

    enable_windows_support = parameter(
        default=True,
        description="Enable patch of all windows non-compatible calls at airflow.",
    )[bool]

    # Databand Executor
    webserver_url = parameter(
        default=None,
        description="Set the URL of airflow webserver to be used by local runs.",
    )[str]

    optimize_airflow_db_access = parameter(
        description="Enable all Airflow database access optimizations."
    )[bool]

    dbnd_pool = parameter(
        description="Separate pool for Databand tasks.", default="dbnd_pool"
    )[str]

    # enabled by optimize_airflow_db_access
    disable_db_ping_on_connect = parameter(
        description="Optimize DB access performance by disabling standard airflow ping "
        "on every sqlalchmey connection initialization."
    )[bool]

    disable_dag_concurrency_rules = parameter(
        description="Enable optimizing database access performance by disabling"
        " DAG and task concurrency checks"
    )[bool]

    dbnd_dag_concurrency = parameter(
        description="Set Concurrency for dbnd ad-hoc DAGs"
    )[int]

    remove_airflow_std_redirect = parameter(
        default=False,
        description="Remove airflow stdout/stderr redirection into logger on DBND operator run. "
        "(redirect can cause crash due to loopback between streams)",
    )[bool]

    clean_zombies_during_backfill = parameter(
        default=False,
        description="Launch additional job during backfill to clean up stalled tasks (zombies).",
    )[bool]

    clean_zombie_task_instances = parameter(
        default=True,
        description="Mark all zombie task_instances of the current run as FAILED (or UP_FOR_RETRY).",
    )[bool]

    # dbnd-airflow command
    auto_add_versioned_dags = parameter(
        default=True,
        description="Enable automatically adding versioned dag support to dbnd-airflow command.",
    )
    auto_add_scheduled_dags = parameter(
        default=True,
        description="Enable automatically adding dbnd scheduled dags to airflow dags on dbnd-airflow command.",
    )
    auto_disable_scheduled_dags_load = parameter(
        default=True,
        description="Enable automatically disabling dbnd scheduled dags load in databand cli.",
    )

    # targets
    use_connections = parameter(
        description="Use the airflow connection to connect to a"
        " cloud environment in databand targets, e.g. `s3://..`, `gcp://..`",
        default=True,
    )[bool]

    def __init__(self, *args, **kwargs):
        super(AirflowConfig, self).__init__(*args, **kwargs)
        if not self.optimize_airflow_db_access:
            self.disable_db_ping_on_connect = False
            self.disable_dag_concurrency_rules = False


username = dbnd_getuser()
default_args = {
    "owner": username,
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def get_dbnd_default_args():
    return default_args.copy()
