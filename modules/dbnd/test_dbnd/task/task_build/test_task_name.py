from dbnd import PipelineTask, config, output, parameter
from dbnd_test_scenarios.test_common.task.factories import TTask


class TTNFirstTask(TTask):
    param = parameter(default="default_param")[str]


class TTNSecondTask(TTNFirstTask):
    pass


class TNamePipe(PipelineTask):
    child = parameter[str]

    first = output
    second = output

    def band(self):
        self.first = TTNFirstTask(task_name="First_%s" % self.child).t_output
        self.second = TTNFirstTask(task_name="Second_%s" % self.child).t_output


class TestTaskNameOverride(object):
    def test_simple(self):
        t = TNamePipe(child="aaa")

        assert str(t.first) != str(t.second)
        assert "First_aaa" in t.first.path
        assert "Second_aaa" in t.second.path

    def test_param_override(self):
        with config({TTNFirstTask.param: "223"}):
            t = TNamePipe(child="aaa")

            assert str(t.first) != str(t.second)
            assert "223" in t.first.task.param
            assert "223" in t.second.task.param

    def test_task_name_priority(self):
        with config({"Second_aaa": {"param": "per_name"}, TTNFirstTask.param: "224"}):
            t = TNamePipe(child="aaa")

            assert str(t.first) != str(t.second)
            assert "224" in t.first.task.param
            assert "per_name" in t.second.task.param
