import pytest

from dbnd import PipelineTask, Task, dbnd_config, dbnd_run_cmd, output, parameter
from dbnd._vendor.pendulum import now
from dbnd.tasks.basics import SimplestTask


class First(Task):
    task_enabled = False
    input_1 = parameter[int]
    result = output[int]

    def run(self):
        self.result = self.input_1


class Pipeline(PipelineTask):
    x = parameter()[str]  # we use this to pass the task_band path to the First
    result = output[int]

    def band(self):
        a = First(input_1=3, task_band=self.x)

        self.result = a.result


@pytest.fixture(scope="function")
def task_band_file(tmpdir_factory):
    fn = tmpdir_factory.mktemp("t").join("task_band.json")
    fn.write('{"input_1": "1"}')
    return fn


class TestBuildTaskWithTaskBand(object):
    def test_build_task_with_task_band_through_constructor(self, task_band_file):
        run = Pipeline(task_version=now(), x=task_band_file.strpath).dbnd_run()
        # accessing the result and check that the used value is the one from the task_band
        assert run.run_executor.result.load("result") == 1

    def test_build_task_with_task_band_through_config(self, task_band_file):
        run = First(input_1=3).dbnd_run()
        assert run.run_executor.result.load("result") == 3

        with dbnd_config({"First": {"task_band": task_band_file.strpath}}):
            run = First(input_1=3).dbnd_run()
        # accessing the result and check that the used value is the one from the task_band
        assert run.run_executor.result.load("result") == 1

    def test_build_task_with_task_band_from_cli(self, task_band_file):
        run = dbnd_run_cmd(["First", "--set", "First.input_1=3"])

        assert run.run_executor.result.load("result") == 3

        run = dbnd_run_cmd(
            [
                "First",
                "--set",
                "First.input_1=3",
                "--set",
                "First.task_band={path}".format(path=task_band_file.strpath),
            ]
        )

        # accessing the result and check that the used value is the one from the task_band
        assert run.run_executor.result.load("result") == 1

    def test_simple_task(self):
        t = SimplestTask(simplest_param=1)
        t.dbnd_run()

        t2 = SimplestTask(simplest_param=2)
        assert str(t2.simplest_output) != str(t.simplest_output)

        t3 = SimplestTask(task_band=t.task_band)

        assert str(t3.simplest_output) == str(t.simplest_output)
