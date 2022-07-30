# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import date, timedelta
from typing import Dict

import pandas as pd

from databand import parameters
from dbnd import PipelineTask, output, parameter, task
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.task.factories import TTask


class DummyTask(TTask):
    float_param = parameters.FloatParameter()
    expected_param = parameters.FloatParameter()

    timedelta_param = parameters.TimeDeltaParameter()
    expected_timedelta_param = parameters.TimeDeltaParameter()

    def run(self):
        assert isinstance(self.float_param, float)
        assert isinstance(self.timedelta_param, timedelta)
        assert self.float_param == self.expected_param
        assert self.timedelta_param == self.expected_timedelta_param
        super(DummyTask, self).run()


class DummyWrapper(PipelineTask):
    defaults = {DummyTask.float_param: "0.1", DummyTask.timedelta_param: "4d"}
    output = output

    def band(self):
        self.output = DummyTask().t_output


@task
def inline_df_without_typing(df_a, df_b):
    return df_a.head(2), df_b.head(2)


@task
def parent_ab_without_typing(iterations=1):
    a = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    b = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    for i in range(iterations):
        # task will be reused the moment we reached dataframe size =2
        a, b = inline_df_without_typing(a, b)
    return a, b


@task
def inline_df_with_typing(df_a, df_b):
    # type: (pd.DataFrame, pd.DataFrame)-> (pd.DataFrame, pd.DataFrame)
    df_a["c1"] = df_a["c1"] * df_b["c1"]
    df_b["c2"] = df_a["c2"] * df_b["c2"]
    return df_a, df_b


@task
def parent_ab_with_typing(iterations=1):
    a = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    b = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    for i in range(iterations):
        # task will be reused the moment we reached dataframe size =2
        a, b = inline_df_with_typing(a, b)
    return a, b


@task
def inline_df_tuple_without_typing(dfs):
    df_a, df_b = dfs
    return df_a.head(2), df_b.head(2)


@task
def parent_ab_tuple_without_typing(iterations=1):
    a = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    b = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    for i in range(iterations):
        a, b = inline_df_tuple_without_typing((a, b))
    return a, b


@task
def inline_df_tuple_with_typing(dfs):
    # type: ((pd.DataFrame, pd.DataFrame))-> (pd.DataFrame, pd.DataFrame)
    df_a, df_b = dfs
    df_a["c1"] = df_a["c1"] * df_b["c1"]
    df_b["c2"] = df_a["c2"] * df_b["c2"]
    return df_a, df_b


@task
def parent_ab_tuple_with_typing(iterations=1):
    a = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    b = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    for i in range(iterations):
        a, b = inline_df_tuple_with_typing((a, b))
    return a, b


@task
def inline_df_dict_without_typing(dfs):
    df_a, df_b = dfs["df_a"], dfs["df_b"]
    return dict(df_a=df_a.head(2), df_b=df_b.head(2))


@task
def parent_ab_dict_without_typing(iterations=1):
    a = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    b = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    for i in range(iterations):
        d = inline_df_dict_without_typing(dict(df_a=a, df_b=b))
        a, b = d["df_a"], d["df_b"]
    return a, b


@task
def inline_df_dict_with_typing(dfs):
    # type: (Dict[str, pd.DataFrame])-> Dict[str, pd.DataFrame]
    df_a, df_b = dfs["df_a"], dfs["df_b"]
    df_a["c1"] = df_a["c1"] * df_b["c1"]
    df_b["c2"] = df_a["c2"] * df_b["c2"]
    return dict(df_a=df_a, df_b=df_b)


@task
def parent_ab_dict_with_typing(iterations=1):
    a = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    b = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])
    for i in range(iterations):
        d = inline_df_dict_with_typing(dict(df_a=a, df_b=b))
        a, b = d["df_a"], d["df_b"]
    return a, b


@task
def inline_dict_of_dict(dfs):
    logging.info("Shape: %s", dfs["df_a"]["df_b"].shape)
    return dfs["df_a"]


@task
def parent_ab_dict_dict(iterations=1):
    a = pd.DataFrame(data=[[1, 1]] * 5, columns=["c1", "c2"])

    result = inline_dict_of_dict(dict(df_a=dict(df_b=a)))
    assert result == dict(df_b=a)
    return result


class TestTaskInlineParsing(object):
    def test_params_inherited_parse(self):
        target = DummyWrapper(
            override={
                DummyTask.expected_param: "0.1",
                DummyTask.expected_timedelta_param: "4d",
            }
        )
        assert_run_task(target)

    def test_task_version_parse(self):
        target = TTask(task_version="now")
        assert target.task_version != "now"

    def test_params_inherited_priority(self):
        target = DummyWrapper(
            override={
                DummyTask.float_param: "0.2",
                DummyTask.expected_param: "0.2",
                DummyTask.expected_timedelta_param: "4d",
            }
        )
        assert_run_task(target)

    def test_params_parse(self):
        class TParseTask(TTask):
            bool_param = parameter[bool]
            int_param = parameter[int]
            float_param = parameters.FloatParameter()
            date_param = parameter[date]
            timedelta_param = parameters.TimeDeltaParameter()

            def run(self):
                assert isinstance(self.float_param, float)
                super(TParseTask, self).run()

        target = TParseTask(
            bool_param="yes",
            int_param="666",
            float_param="123.456",
            date_param="2018-11-10",
            timedelta_param="44d",
        )
        assert_run_task(target)

    def test_task_inline_dataframe_with_types(self):
        parent_ab_with_typing.task(5).dbnd_run()

    def test_task_inline_dataframe_without_types(self):
        parent_ab_without_typing.task(5).dbnd_run()

    def test_task_inline_dataframe_tuple_with_types(self):
        parent_ab_tuple_with_typing.task(5).dbnd_run()

    def test_task_inline_dataframe_tuple_without_types(self):
        parent_ab_tuple_without_typing.task(5).dbnd_run()

    def test_task_inline_dataframe_dict_with_types(self):
        parent_ab_dict_with_typing.task(5).dbnd_run()

    def test_task_inline_dataframe_dict_without_types(self):
        parent_ab_dict_without_typing.task(5).dbnd_run()
