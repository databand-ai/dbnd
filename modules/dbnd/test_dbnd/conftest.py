from __future__ import print_function

import os

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from pytest import fixture

# import dbnd should be first!
import dbnd
import dbnd._core.utils.basics.environ_utils

from dbnd import get_dbnd_project_config, register_config_cls, register_task
from dbnd._core.configuration import environ_config
from dbnd._core.plugin.dbnd_plugins import disable_airflow_plugin
from dbnd_test_scenarios.test_common.task.factories import FooConfig, TConfig
from targets import target


# we want to test only this module
get_dbnd_project_config().is_no_modules = True
# disable DB tracking
os.environ["DBND__CORE__TRACKER"] = "['file', 'console']"

# DISABLE AIRFLOW, we don't test it in this module!
disable_airflow_plugin()
pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
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
    names = ["Bob", "Jessica", "Mary", "John", "Mel"]
    births = [968, 155, 77, 578, 973]
    df = pd.DataFrame(data=list(zip(names, births)), columns=["Names", "Births"])
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
