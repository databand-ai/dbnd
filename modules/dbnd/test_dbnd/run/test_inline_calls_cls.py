from dbnd import data, output, parameter, run_task, task
from dbnd.testing import assert_run_task


@task
def calc_value(value=1.0):
    # type: (float)-> float

    value_task = inline_task_value.t(value=value)
    value_task.dbnd_run()
    return value_task.result.read_pickle() + 0.1


@task
def inline_task_value(value=1.0):
    # type: (float)-> float
    return value + 0.1


class TestSpawnTasks(object):
    def test_simple_child(self):
        target = calc_value.t(0.5)
        assert_run_task(target)
        assert target.result.read_obj()
