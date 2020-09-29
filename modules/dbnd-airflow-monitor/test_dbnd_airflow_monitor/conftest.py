from __future__ import print_function

import os

from dbnd._core.configuration.environ_config import reset_dbnd_project_config


home = os.path.abspath(
    os.path.normpath(os.path.join(os.path.dirname(__file__), "home"))
)
os.environ["DBND_HOME"] = home
os.environ["AIRFLOW_HOME"] = home
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
reset_dbnd_project_config()
