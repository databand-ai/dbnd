# © Copyright Databand.ai, an IBM Company 2022

import os

from dbnd._core.configuration.environ_config import ENV_DBND_HOME, ENV_DBND_SYSTEM
from dbnd._core.log import dbnd_log_init_msg


def _initialize_airflow_home():
    ENV_AIRFLOW_HOME = "AIRFLOW_HOME"

    if ENV_AIRFLOW_HOME in os.environ:
        # user settings - we do nothing
        dbnd_log_init_msg(
            "Found user defined AIRFLOW_HOME at %s" % os.environ[ENV_AIRFLOW_HOME]
        )
        return

    for dbnd_airflow_home in [
        os.path.join(os.environ[ENV_DBND_SYSTEM], "airflow"),
        os.path.join(os.environ[ENV_DBND_HOME], ".airflow"),
    ]:
        if not os.path.exists(dbnd_airflow_home):
            continue

        dbnd_log_init_msg(
            "Found airflow home folder at DBND, setting AIRFLOW_HOME to %s"
            % dbnd_airflow_home
        )
        os.environ[ENV_AIRFLOW_HOME] = dbnd_airflow_home
