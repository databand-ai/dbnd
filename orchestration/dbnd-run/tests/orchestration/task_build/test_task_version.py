# © Copyright Databand.ai, an IBM Company 2022

from dbnd import PipelineTask, band, namespace, output, parameter, task
from dbnd_test_scenarios.test_common.task.factories import FooConfig, TTask


namespace("n_tv")


class FirstTask(TTask):
    foo = parameter(default="FooConfig")[FooConfig]
    param = parameter(default="from_first")[str]


class SecondATask(FirstTask):
    param = "from_second"


class SecondBTask(FirstTask):
    pass


class InnerPipeTask(PipelineTask):
    second_b = output

    def band(self):
        self.second_b = SecondBTask(task_name="innerB", param="from_pipe")


class BigPipeTask(PipelineTask):
    second_a = output
    second_b = output
    inner_second_b = output

    def band(self):
        self.second_a = SecondATask().t_output
        self.second_b = SecondBTask().t_output
        self.inner_second_b = InnerPipeTask().second_b


namespace()


def assert_task_version(expected, task):
    all_tasks = task.ctrl.task_dag.subdag_tasks()
    actual = {t.task_name: t.task_version for t in all_tasks}
    print(actual)
    assert expected == actual


class TestTaskVersion(object):
    def test_sanity(self):
        target = FirstTask()

        assert_task_version({"n_tv.FirstTask": "1"}, target)

    def test_default_vertion(self):
        target = BigPipeTask()

        assert_task_version(
            {
                "n_tv.SecondATask": "1",
                "n_tv.SecondBTask": "1",
                "innerB": "1",
                "n_tv.InnerPipeTask": "1",
                "n_tv.BigPipeTask": "1",
            },
            target,
        )

    def test_force_version(self):
        target = BigPipeTask(task_version=2)

        assert_task_version(
            {
                "n_tv.SecondATask": "2",
                "n_tv.SecondBTask": "2",
                "innerB": "2",
                "n_tv.InnerPipeTask": "2",
                "n_tv.BigPipeTask": "2",
            },
            target,
        )

    def test_force_specific_vertion(self):
        target = BigPipeTask(override={SecondATask.task_version: 2})

        assert_task_version(
            {
                "n_tv.SecondATask": "2",
                "n_tv.SecondBTask": "1",
                "innerB": "1",
                "n_tv.InnerPipeTask": "1",
                "n_tv.BigPipeTask": "1",
            },
            target,
        )

    def test_force_pipe_version(self):
        target = BigPipeTask(override={InnerPipeTask.task_version: 2})

        assert_task_version(
            {
                "n_tv.SecondATask": "1",
                "n_tv.SecondBTask": "1",
                "innerB": "2",
                "n_tv.InnerPipeTask": "2",
                "n_tv.BigPipeTask": "1",
            },
            target,
        )


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
