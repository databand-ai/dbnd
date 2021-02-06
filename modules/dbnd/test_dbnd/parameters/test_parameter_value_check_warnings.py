import pandas as pd

from dbnd import pipeline, task
from targets.value_meta import ValueMetaConf


@task
def t_df():
    # type: ()-> pd.DataFrame
    df = pd.DataFrame(
        data=list(zip(["Bob", "Jessica"], [968, 155])), columns=["Names", "Births"]
    )
    return df


@task
def t_df_df(df):
    # type: (pd.DataFrame)-> pd.DataFrame
    return df


@pipeline
def t_no_warnings():
    t1 = t_df()
    t2 = t_df_df(t1)
    return t2


class TestParameterTypeCheckWarnings(object):
    def test_no_warnings_on_target(self):
        actual = t_no_warnings.task()
        assert not actual.result.task._params.get_param_value("df").warnings
