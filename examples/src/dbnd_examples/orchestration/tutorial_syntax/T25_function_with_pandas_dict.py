import logging

from typing import Dict

import pandas as pd

from dbnd import pipeline, task


logger = logging.getLogger(__name__)


@task
def f_pandas_df(i: int) -> pd.DataFrame:
    return pd.DataFrame(
        data=list(zip(["Bob", "Jessica"], [i, i])), columns=["Names", "Births"]
    )


@task
def f_df_dict(df_dict):
    # type: (Dict[str, pd.DataFrame]) -> pd.DataFrame
    values = df_dict.values()
    return pd.concat(values)


@pipeline
def f_pipe_df_dict():
    df_dict = {}
    for i in range(5):
        df_dict[str(i)] = f_pandas_df(i)
    df_sum = f_df_dict(df_dict)

    return df_sum


if __name__ == "__main__":
    f_pipe_df_dict.dbnd_run()
