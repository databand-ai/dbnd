from dbnd import dbnd_run_cmd, parameter
from dbnd_test_scenarios.test_common.task.factories import TTask


class Baz(TTask):
    bool = parameter(default=False)[bool]


class BazTrue(TTask):
    bool = parameter.value(True)


class TBoolWithDefault(TTask):
    x = parameter.value(default=True)


class TestTaskBoolParameters(object):
    def test_bool_false_default(self):
        result = dbnd_run_cmd(["Baz"])
        assert result.task.bool is False

    def test_bool_true(self):
        result = dbnd_run_cmd(["Baz", "-r", "bool=True"])
        assert result.task.bool is True

    def test_bool_false_cmdline(self):
        result = dbnd_run_cmd(["BazTrue", "-r", "bool=False"])
        assert result.task.bool is False

    def test_bool_true_default(self):
        result = dbnd_run_cmd(["BazTrue"])
        assert result.task.bool is True

    def test_bool_default_true(self):
        assert TBoolWithDefault().x

    def test_bool_coerce(self):
        assert True is TBoolWithDefault(x="yes").x

    def test_bool_no_coerce_none(self):
        assert TBoolWithDefault(x=None).x is None
