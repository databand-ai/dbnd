# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import os

# import dbnd should be first!
import dbnd

from dbnd._core.context.use_dbnd_run import disable_airflow_package


# disable DB tracking
os.environ["DBND__CORE__TRACKER"] = "['console']"

# DISABLE AIRFLOW, we don't test it in this module!
disable_airflow_package()

pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]
__all__ = ["dbnd"]
