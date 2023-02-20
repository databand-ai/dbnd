# © Copyright Databand.ai, an IBM Company 2022

from typing import List

import pandas as pd

from dbnd import log_dataframe, task


@task
def load_from_sql_data(sql_conn_str, query) -> pd.DataFrame:
    import sqlalchemy

    engine = sqlalchemy.create_engine(sql_conn_str)
    return pd.read_sql(query, engine)


@task
def join_data(raw_data: List[pd.DataFrame]) -> pd.DataFrame:
    result = raw_data.pop(0)
    for d in raw_data:
        result = result.merge(d, on="id")
    log_dataframe(result)
    return result
