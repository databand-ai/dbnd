from dbnd import PipelineTask, namespace, output, parameter
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
    all_tasks = task.task_dag.subdag_tasks()
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
