#### DOC START

import numpy
import pandas as pd

from dbnd import pipeline, task


@task
def f_pandas_numpy(v_df: pd.DataFrame, v_series: pd.Series, v_np: numpy.ndarray) -> str:
    assert isinstance(v_df, pd.DataFrame)
    assert isinstance(v_series, pd.Series)
    assert isinstance(v_np, numpy.ndarray)

    return "df:%s series:%s np:%s" % (v_df.shape, v_series.shape, v_np.shape)


@task(result=("df", "series", "np"))
def f_create_pandas_numpy() -> (pd.DataFrame, pd.Series, numpy.ndarray):
    s = pd.Series([1, 3, 5, 8])
    df = pd.DataFrame(
        data=list(zip(["Bob", "Jessica"], [968, 155])), columns=["Names", "Births"]
    )

    return df, s, df.values


@pipeline
def f_run_pandas_numpy():
    simple = f_create_pandas_numpy()
    pandas_numpy_str = f_pandas_numpy(*simple)

    return (pandas_numpy_str,)


@task
def f_assert(pandas_numpy_str):
    assert "df:(2, 2) series:(4,) np:(2, 2)" == pandas_numpy_str

    return "OK"


@pipeline
def f_test_pandas_numpy_flow():
    all_simple = f_run_pandas_numpy()
    return f_assert(*all_simple)


#### DOC END
