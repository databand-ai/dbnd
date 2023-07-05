# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict

import pandas as pd
import six

from dbnd import band, parameter, task
from dbnd_run.task.task import Task


class _MyObject(object):
    pass


@task
def t_df(p):
    # type: (float)-> pd.DataFrame
    return pd.DataFrame(data=[[p, 1], [2, 2]], columns=["c1", "c2"])


@task
def t_df_dict_producer(p):
    # type: (int) -> Dict[str, pd.DataFrame]
    result = {}
    for key in ["a", "b"]:
        result[key] = pd.DataFrame(data=[[p, 1], [2, 2]], columns=[key, "c2"])
    return result


@task
def t_df_dict_consumer(df_dict):
    # type: ( Dict[str, pd.DataFrame]) -> str
    for key, value in six.iteritems(df_dict):
        assert isinstance(value, pd.DataFrame)
    return "ok"


@task
def t_df_dict_dict_consumer(df_dict):
    # type: ( Dict[str, Dict[str, pd.DataFrame]]) -> str
    for key, nested_value in six.iteritems(df_dict):
        for sub_key, value in six.iteritems(nested_value):
            assert isinstance(value, pd.DataFrame)
    return "ok"


@band
def t_band_dict():
    # type: () -> (str,str)
    df_dict = {str(k): t_df(k) for k in range(3)}
    dict_t = t_df_dict_consumer(df_dict=df_dict)

    dict_dict_t = t_df_dict_dict_consumer(df_dict={"nested": df_dict})

    return dict_dict_t, dict_t


class TestParameterDataFrame(object):
    def test_nested_dict(self):
        tr = t_band_dict.dbnd_run()
        t_df_task: Task = tr.task.result.task
        logging.warning(t_df_task.ctrl.task_dag.downstream)

    def test_inplace_dict(self):
        @band
        def t_band_inline_dict():
            # type: () -> str
            df_dict = t_df_dict_producer(1)
            return t_df_dict_consumer(df_dict=df_dict)

        tr = t_band_inline_dict.dbnd_run()
        t_df_task = tr.task.result.task  # type: Task
        logging.warning(t_df_task.ctrl.task_dag.downstream)

    def test_excel(self):
        @task(result=parameter.excel)
        def dump_as_excel_table():
            # type: ()-> pd.DataFrame
            df = pd.DataFrame(
                data=list(zip(["Bob", "Jessica"], [968, 155])),
                columns=["Names", "Births"],
            )
            return df

        dump_as_excel_table.dbnd_run()
