# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict

import six

from dbnd import band, task
from dbnd._core.task import Task


class _MyObject(object):
    pass


@task
def t_df(p):
    # type: (float)-> int
    return int(p)


@task
def t_df_dict_consumer(df_dict):
    # type: ( Dict[str, Dict[str, int]]) -> str
    for key, nested_value in six.iteritems(df_dict):
        for sub_key, value in six.iteritems(nested_value):
            assert isinstance(value, int)
    return "ok"


@band
def t_band_dict():
    # type: () -> str
    nested = {str(k): t_df(k) for k in range(3)}
    df_dict = {"nested": nested}
    return t_df_dict_consumer(df_dict=df_dict)


class TestNestedTypes(object):
    def test_nested_dict(self):
        tr = t_band_dict.dbnd_run()
        t_df_task = tr.task.result.task  # type: Task
        logging.warning(t_df_task.ctrl.task_dag.downstream)
