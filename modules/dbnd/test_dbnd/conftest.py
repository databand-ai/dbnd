# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from pytest import fixture

# import dbnd should be first!
import dbnd
import dbnd._core.utils.basics.environ_utils

from dbnd import register_config_cls, register_task
from dbnd._core.configuration.environ_config import (
    ENV_DBND__DISABLE_PLUGGY_ENTRYPOINT_LOADING,
    ENV_DBND__NO_MODULES,
    ENV_DBND__NO_PLUGINS,
    reset_dbnd_project_config,
)
from dbnd._core.plugin.dbnd_plugins import disable_airflow_plugin
from dbnd._core.utils.basics.environ_utils import set_on
from dbnd.testing.test_config_setter import add_test_configuration
from dbnd_test_scenarios.test_common.task.factories import FooConfig, TConfig
from targets import target


# we want to test only this module
# However, this import runs in separate space from test run -> this global will not be visible to your test
# get_dbnd_project_config().is_no_modules = True

# if enabled will pring much better info on tests
# os.environ["DBND__VERBOSE"] = "True"
set_on(ENV_DBND__NO_MODULES)
set_on(ENV_DBND__NO_PLUGINS)
set_on(ENV_DBND__DISABLE_PLUGGY_ENTRYPOINT_LOADING)

# DISABLE AIRFLOW, we don't test it in this module!
disable_airflow_plugin()

# all env changes should be active for current project config
reset_dbnd_project_config()

pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]
__all__ = ["dbnd"]

try:
    import matplotlib

    # see:
    # https://stackoverflow.com/questions/37604289/tkinter-tclerror-no-display-name-and-no-display-environment-variable
    # https://markhneedham.com/blog/2018/05/04/python-runtime-error-osx-matplotlib-not-installed-as-framework-mac/
    matplotlib.use("Agg")
except ModuleNotFoundError:
    pass

# by default exclude tests marked with following marks from execution:
markers_to_exlude_by_default = ["dbnd_integration"]


def pytest_configure(config):
    add_test_configuration(__file__)
    markexpr = getattr(config.option, "markexpr", "")
    marks = [markexpr] if markexpr else []
    for mark in markers_to_exlude_by_default:
        if mark not in markexpr:
            marks.append("not %s" % mark)
    new_markexpr = " and ".join(marks)
    setattr(config.option, "markexpr", new_markexpr)
    register_task(TConfig)
    register_config_cls(FooConfig)


@pytest.fixture
def matplot_figure():
    import matplotlib.pyplot as plt

    fig = plt.figure()
    ax1 = fig.add_subplot(2, 2, 1)
    ax1.hist(np.random.randn(100), bins=20, alpha=0.3)
    ax2 = fig.add_subplot(2, 2, 2)
    ax2.scatter(np.arange(30), np.arange(30) + 3 * np.random.randn(30))
    fig.add_subplot(2, 2, 3)
    return fig


@pytest.fixture
def pandas_data_frame():
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
def df_categorical():
    return pd.DataFrame(
        {"A": [1, 2, 3], "B": pd.Series(list("xyz")).astype("category")}
    )


@fixture
def file_on_disk(tmpdir):
    t = target(tmpdir / "file_on_disk.txt")
    t.write("file data")
    return t


@pytest.fixture
def pandas_data_frame_index(pandas_data_frame):
    return pandas_data_frame.set_index("Names")


@pytest.fixture
def pandas_data_frame_on_disk(tmpdir, pandas_data_frame):
    t = target(tmpdir / "df.parquet")
    t.write_df(pandas_data_frame)
    return pandas_data_frame, t


@pytest.fixture
def numpy_array():
    return np.array([968, 155, 77, 578, 973])


@fixture
def partitioned_data_target_date():
    return datetime.date(year=2018, month=9, day=3)
