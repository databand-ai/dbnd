# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

# inline conftest
import logging
import os
import sys

import pytest

# should be before any databand import!
from dbnd.testing.test_config_setter import add_test_configuration


#### DBND HOME
dbnd_system_home = os.path.abspath(
    os.path.normpath(os.path.join(os.path.dirname(__file__), "airflow_home", ".dbnd"))
)  # isort:skip

os.environ["DBND_SYSTEM"] = dbnd_system_home  # isort:skip

############
#  AIRFLOW
# we need it to run before `airflow` import
airflow_home = os.path.abspath(
    os.path.normpath(os.path.join(os.path.dirname(__file__), "airflow_home"))
)  # isort:skip
os.environ["AIRFLOW_HOME"] = airflow_home  # isort:skip
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"  # isort:skip
# we can't access `airlfow` package for version, import will load config
# in tox we will have AIRFLOW_VERSION, so there are fewer chances for config conflicts
os.environ["AIRFLOW_CONFIG"] = os.path.join(
    airflow_home, "airflow%s.cfg" % os.environ.get("AIRFLOW_VERSION", "")
)  # isort:skip

# import dbnd should be first!


# make test_dbnd available
from dbnd.testing.helpers import dbnd_module_path  # isort:skip

# import dbnd should be first!

sys.path.append(dbnd_module_path())

logger = logging.getLogger(__name__)

pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]


def pytest_configure(config):
    add_test_configuration(__file__)


@pytest.fixture(autouse=True)
def dbnd_env_per_test(databand_pytest_env):
    yield databand_pytest_env


@pytest.fixture
def pandas_data_frame():
    import pandas as pd

    df = pd.DataFrame(
        {
            "Names": pd.Series(["Bob", "Jessica", "Mary", "John", "Mel"], dtype="str"),
            "Births": pd.Series([968, 155, 77, 578, 973], dtype="int"),
            # "Weights": pd.Series([12.3, 23.4, 45.6, 56.7, 67.8], dtype="float"),
            "Married": pd.Series([True, False, True, False, True], dtype="bool"),
        }
    )
    return df


@pytest.fixture
def numpy_array():
    import numpy as np

    return np.array([968, 155, 77, 578, 973])
