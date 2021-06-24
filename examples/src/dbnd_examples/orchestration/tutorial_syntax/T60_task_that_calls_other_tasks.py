import pandas as pd

from dbnd import task


@task
def func_return_df():
    return pd.DataFrame(data=[[3, 1]], columns=["c1", "c2"])


@task
def func_gets(df):
    return df


@task
def func_pipeline(p: int):
    df = pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"])
    d1 = func_gets(df)
    d2 = func_gets(d1)
    return d2


@task
def func_pipeline2(p: int):
    df = func_return_df()
    d1 = func_gets(df)
    return d1


if __name__ == "__main__":
    import os

    os.environ["DBND__TRACKING"] = "true"
    func_pipeline2(4)
