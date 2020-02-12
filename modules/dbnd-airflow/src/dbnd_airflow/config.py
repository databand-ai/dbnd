from datetime import datetime, timedelta

from dbnd import parameter
from dbnd._core.task import Config
from dbnd._core.utils.platform.windows_compatible.getuser import dbnd_getuser


class AirflowConfig(Config):
    """Apache Airflow (currently a part of Databand)"""

    _conf__task_family = "airflow"

    optimize_airflow_db_access = parameter(
        description="Enable all Airflow DB access optimizations"
    )[bool]

    # enabled by optimize_airflow_db_access
    disable_db_ping_on_connect = parameter(
        description="Optimize DB access performance by disabling starndard airflow ping "
        "on every sqlalchmey connectiion initializaiton"
    )[bool]

    disable_dag_concurrency_rules = parameter(
        description="Optimize DB access performance by disabling"
        " dag and task concurrency checks"
    )[bool]

    dbnd_dag_concurrency = parameter(description="Concurrency for dbnd ad-hoc dags")[
        int
    ]

    remove_airflow_std_redirect = parameter(
        default=False,
        description="Remove airflow stdout/stderr redirection into logger on DBND operator run "
        "(redirect can cause crash due to loopback between streams",
    )[bool]

    sql_alchemy_conn = parameter(
        description="key used by airflow to encrypt connections credentials "
        "(use 'dbnd' for using value from `core.fernet_key`)",
        default=None,
    )[str]
    fernet_key = parameter(
        description="key used by airflow to encrypt connections credentials "
        "(use 'dbnd' for using value from `core.fernet_key`)",
        default=None,
    )[str]
    auto_add_versioned_dags = parameter(
        default=True,
        description="Auto add versioned dag support to dbnd-airflow command",
    )
    auto_add_scheduled_dags = parameter(
        default=True,
        description="Auto add dbnd scheduled dags to airflow dags on dbnd-airflow command ",
    )
    auto_disable_scheduled_dags_load = parameter(
        default=True,
        description="Auto disable dbnd scheduled dags load in databand clie",
    )
    webserver_url = parameter(
        default=None, description="URL of airflow webserver used by local runs"
    )[str]

    use_connections = parameter(
        description="use the airflow connection to connect to a cloud environment",
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
