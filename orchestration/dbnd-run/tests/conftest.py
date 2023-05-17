# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

# inline conftest
import logging
import os

import pytest

from dbnd._core.configuration.environ_config import (
    ENV_DBND__DISABLE_PLUGGY_ENTRYPOINT_LOADING,
    ENV_DBND__ORCHESTRATION_MODE,
)
from dbnd._core.utils.basics.environ_utils import set_on

# should be before any databand import!
from dbnd.testing.test_config_setter import add_test_configuration
from targets import target


logger = logging.getLogger(__name__)

set_on(ENV_DBND__ORCHESTRATION_MODE)
set_on(ENV_DBND__DISABLE_PLUGGY_ENTRYPOINT_LOADING)

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

pytest_plugins = [
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
    "dbnd.orchestration.testing.pytest_dbnd_run_plugin",
]


### MAIN FIXTURE, USED BY ALL TESTS ###
@pytest.fixture(autouse=True)
def dbnd_env_per_test(dbnd_run_pytest_env):
    add_test_configuration(__file__)
    yield dbnd_run_pytest_env


def pytest_configure(config):
    add_test_configuration(__file__)


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
def pandas_data_frame_on_disk(tmpdir, pandas_data_frame):
    t = target(tmpdir / "df.parquet")
    t.write_df(pandas_data_frame)
    return pandas_data_frame, t


@pytest.fixture
def numpy_array():
    import numpy as np

    return np.array([968, 155, 77, 578, 973])
