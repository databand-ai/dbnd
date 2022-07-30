# © Copyright Databand.ai, an IBM Company 2022

import pandas as pd
import pytest

from dbnd import band, task
from targets.types import PathStr


class _MyObject(str):
    pass


@task
def inmemory_run(obj, df):
    # type: (object, pd.DataFrame) -> pd.DataFrame
    return df


@band
def inmemory_band(obj, df):
    # type: (object, pd.DataFrame) -> pd.DataFrame
    # noinspection PyTypeChecker
    return inmemory_run(obj=obj, df=df)


@band
def inmemory_pipeline():
    obj = {_MyObject(): i for i in range(100)}
    df = pd.DataFrame(data=[[1, 1]], columns=["c1", "c2"])
    return inmemory_band(obj, df)


class TestTaskInMemoryParameters(object):
    def test_inmemory_band(self):
        inmemory_pipeline.dbnd_run()

    def test_wrong_types_for_inmemory(self):
        @task
        def inmemory_path(obj, df_as_path):
            # type: (PathStr, PathStr) -> object
            return ""

        @band
        def inmemory_pipeline_path_str():
            obj = {_MyObject(): i for i in range(100)}
            df = pd.DataFrame(data=[[1, 1]], columns=["c1", "c2"])
            return inmemory_path(obj=obj, df_as_path=df)

        with pytest.raises(Exception):
            inmemory_pipeline_path_str.dbnd_run()
