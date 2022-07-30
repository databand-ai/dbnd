# © Copyright Databand.ai, an IBM Company 2022

from typing import NamedTuple

import pandas as pd

from pandas import DataFrame

from dbnd import task


@task(result=("features", "scores"))
def f_returns_two_dataframes_v1(p: int) -> (DataFrame, DataFrame):
    return (
        pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
        pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
    )


@task(result="features,scores")
def f_returns_two_dataframes_v2(p: int) -> (DataFrame, DataFrame):
    return (
        pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
        pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
    )


@task
def f_returns_two_dataframes_no_hint(p: int) -> (DataFrame, DataFrame):
    return (
        pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
        pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
    )


FeatureStore = NamedTuple("FeatureStore", features=DataFrame, scores=DataFrame)


@task
def f_returns_two_dataframes_named_tuple_v1(p: int) -> FeatureStore:
    return FeatureStore(
        pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
        pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
    )


@task
def f_returns_two_dataframes_named_tuple_v2(
    p: int,
) -> NamedTuple(
    "FeatureStore", fields=[("features", DataFrame), ("scores", DataFrame)]
):
    return (
        pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
        pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
    )


@task
def f_multiple_outputs_py2():
    # type: ()-> (pd.DataFrame, pd.DataFrame)
    return (
        pd.DataFrame(data=[[1, 1]], columns=["c1", "c2"]),
        pd.DataFrame(data=[[1, 1]], columns=["c1", "c2"]),
    )


@task
def f_returns_huge_dataframe(p: int) -> DataFrame:
    return pd.DataFrame(data=[[p] * 100000])
