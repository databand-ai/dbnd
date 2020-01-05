import json
import logging

import dbnd

from dbnd import config, dbnd_run_cmd, output, parameter
from dbnd.testing.helpers_pytest import skip_on_windows
from test_dbnd.factories import TTask
from test_dbnd.scenarios import scenario_path


logger = logging.getLogger(__name__)


class MyConfig(dbnd.Config):
    mc_p = parameter[int]
    mc_q = parameter.value(73)


class MyConfigTester(dbnd.Task):
    t_output = output.json[object]

    def run(self):
        config.log_current_config(sections=["MyConfig"], as_table=True)
        c = MyConfig()
        self.t_output = [c.mc_p, c.mc_q]


class TestTaskCliSetConfig(object):
    def test_from_extra_config(self):
        class MyTaskWithConfg(TTask):
            parameter_with_config = parameter[str]

            def __init__(self, **kwargs):
                super(MyTaskWithConfg, self).__init__(**kwargs)

            def run(self):
                super(MyTaskWithConfg, self).run()
                assert self.parameter_with_config == "value_from_config"

        assert dbnd_run_cmd(
            "MyTaskWithConfg --conf-file %s"
            % scenario_path("config_files/test_cfg_switch.cfg")
        )

    @skip_on_windows
    def test_from_config_cli_inline(self):
        class MyTaskWithConfgInline(TTask):
            parameter_with_config = parameter[str]

            def __init__(self, **kwargs):
                super(MyTaskWithConfgInline, self).__init__(**kwargs)

            def run(self):
                super(MyTaskWithConfgInline, self).run()
                assert self.parameter_with_config == "value_from_inline"

        json_value = json.dumps(
            {"MyTaskWithConfgInline": {"parameter_with_config": "value_from_inline"}}
        )
        assert dbnd_run_cmd("MyTaskWithConfgInline -s '%s'" % json_value)

    def test_use_config_class_1(self):
        result = dbnd_run_cmd(["MyConfigTester", "-s", "MyConfig.mc_p=99"])
        actual = result.task.t_output.load(object)
        assert actual == [99, 73]

    def test_use_config_class_more_args(self):
        result = dbnd_run_cmd("MyConfigTester -s MyConfig.mc_p=99 -s MyConfig.mc_q=42")
        actual = result.task.t_output.load(object)
        assert actual == [99, 42]

    def test_use_config_class_with_configuration(self):
        result = dbnd_run_cmd(
            [
                "MyConfigTester",
                "--set",
                json.dumps({"MyConfig": {"mc_p": "123", "mc_q": "345"}}),
            ]
        )
        actual = result.task.t_output.load(object)
        assert actual == [123, 345]

    def test_param_override(self):
        result = dbnd_run_cmd(
            [
                "MyConfigTester",
                "-o",
                "{'MyConfig.mc_p': '222', 'MyConfig.mc_q': '223'}",
                "-s",
                "{'MyConfig.mc_p': '999', 'MyConfig.mc_q': '888'}",
            ]
        )

        actual = result.task.t_output.load(object)
        assert actual == [222, 223]

    def test_param_override_2(self):
        result = dbnd_run_cmd(
            [
                "MyConfigTester",
                "-s",
                "{'MyConfig.mc_p': '999', 'MyConfig.mc_q': '888'}",
                "-o",
                "{'MyConfig.mc_p': '222', 'MyConfig.mc_q': '223'}",
            ]
        )

        actual = result.task.t_output.load(object)
        assert actual == [222, 223]
