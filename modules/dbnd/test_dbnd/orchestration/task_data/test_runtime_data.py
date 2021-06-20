import datetime

from typing import Dict, List

import pandas as pd

from dbnd import band, task
from targets.types import Path


@task
def t1_producer(a_str):
    # type: (str)->(str, datetime.datetime, datetime.timedelta, int)

    now = datetime.datetime.now()
    return ["strX", now, datetime.timedelta(seconds=1), 1]


@task
def t1_consumer(a_str, b_datetime, c_timedelta, d_int):
    # type: (str, datetime.datetime, datetime.timedelta, int) -> DataFrame
    return pd.DataFrame(data=[[1, 1]], columns=["c1", "c2"])


@band
def t1_producer_consumer():
    tp = t1_producer("1")
    tc = t1_consumer(*tp)
    return tc


@task
def t2_df(a=1):
    # type: (...)-> pd.DataFrame
    return pd.DataFrame(data=[[a, a]], columns=["c1", "c2"])


######
# Path


@task
def t2_path_list(list_of_paths):
    # type: (List[Path]) -> object

    for l in list_of_paths:
        assert isinstance(l, Path)
    return list_of_paths[-1]


@band
def t2_band_paths():
    ll = [t2_df(i) for i in range(2)]
    return t2_path_list(ll)


###
# DataFrame


@task
def t2_df_list(list_of_df):
    # type: (List[pd.DataFrame]) -> pd.DataFrame

    for l in list_of_df:
        assert isinstance(l, pd.DataFrame)
    return list_of_df[-1]


@band
def t2_band_df():
    ll = [t2_df(i) for i in range(2)]
    return t2_df_list(ll)


######
# Dict[str, Path]


@task
def t2_df_dict(dict_of_df):
    # type: (Dict[str, pd.DataFrame]) -> pd.DataFrame

    for l in dict_of_df.values():
        assert isinstance(l, pd.DataFrame)
    return list(dict_of_df.values())[-1]


@band
def t2_band_dict_df():
    ll = {str(i): t2_df(i) for i in range(2)}
    return t2_df_dict(ll)


class TestTaskRuntimeData(object):
    def test_simple_types(self):
        t1_producer_consumer.dbnd_run()

    def test_path_types(self):
        t2_band_paths.dbnd_run()

    def test_df_types(self):
        t2_band_df.dbnd_run()

    def test_df_dict(self):
        t2_band_dict_df.dbnd_run()
