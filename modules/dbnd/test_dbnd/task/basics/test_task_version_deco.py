from dbnd import band, data, output, parameter, task


@task
def t1():
    return "t1"


@task
def t2():
    return "t2"


@band
def inner_pipe():
    return t1(task_name="t1_inner")


@band
def pipe():
    return inner_pipe(), t1(), t2()


def assert_task_version(expected, task):
    all_tasks = task.ctrl.task_dag.subdag_tasks()
    actual = {t.task_name: t.task_version for t in all_tasks}
    print(actual)
    assert expected == actual


class TestTaskVersionDecorator(object):
    def test_sanity(self):
        target = t1.t()

        assert_task_version({"t1": "1"}, target)

    def test_default_vertion(self):
        target = pipe.t()

        assert_task_version(
            {"t1_inner": "1", "inner_pipe": "1", "t1": "1", "t2": "1", "pipe": "1"},
            target,
        )

    def test_force_version(self):
        target = pipe.t(task_version=2)

        assert_task_version(
            {"t1_inner": "2", "inner_pipe": "2", "t1": "2", "t2": "2", "pipe": "2"},
            target,
        )

    def test_force_specific_vertion(self):
        target = pipe.t(override={t2.t.task_version: 2})

        assert_task_version(
            {"t1_inner": "1", "inner_pipe": "1", "t1": "1", "t2": "2", "pipe": "1"},
            target,
        )

    def test_force_pipe_version(self):
        target = pipe.t(override={inner_pipe.t.task_version: 2})

        assert_task_version(
            {"t1_inner": "2", "inner_pipe": "2", "t1": "1", "t2": "1", "pipe": "1"},
            target,
        )
