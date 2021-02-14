import pandas

from pytest import fixture

from dbnd import current_task_run, task


@task
def task_wire_with_str(str_input, postfix="->with_str"):
    return str_input + postfix


@task
def task_3_wire_with_str(str_input):
    a = task_wire_with_str(str_input)
    b = task_wire_with_str(a)
    return task_wire_with_str(b)


@task
def task_wire_with_df(df):
    # type: (pandas.DataFrame)->pandas.DataFrame
    df = df.copy()
    df["last_task"] = current_task_run().task_af_id
    return df


@task
def task_3_wire_with_df(df):
    # type: (pandas.DataFrame)->pandas.DataFrame
    a = task_wire_with_df(df)
    b = task_wire_with_df(a)
    return task_wire_with_df(b)


class TestDataLineageTracking(object):
    @fixture(autouse=True)
    def _tracking_context(self, set_tracking_context, set_verbose_mode):
        pass

    def test_simple_type_tracking(self):
        task_3_wire_with_str("test_input")

    def test_df(self, pandas_data_frame):
        task_3_wire_with_df(pandas_data_frame)
