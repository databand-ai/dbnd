from __future__ import print_function

import json
import logging

import pytest

import dbnd

from dbnd import PythonTask, Task, config, dbnd_run_cmd, output, parameter
from dbnd._core.errors import (
    DatabandRunError,
    TaskClassAmbigiousException,
    TaskClassNotFoundException,
)
from dbnd._vendor.snippets.edit_distance import get_editdistance
from dbnd.testing.helpers_pytest import run_locally__raises, skip_on_windows
from dbnd_test_scenarios import scenario_path
from dbnd_test_scenarios.test_common.task.factories import TTask


logger = logging.getLogger(__name__)


class TCmdTask(TTask):
    x = parameter.value("1")
    y = parameter.value("default")


class TCmdPipe(PythonTask):
    x = parameter.value("1")
    y = parameter.value("default")

    def band(self):
        return TCmdTask()


class TConfigTask(TTask):
    def run(self):
        if self.t_param != "123":
            raise Exception("t param has wrong value %s" % self.t_param)
        return super(TConfigTask, self).run()


class TConfigNoDefault(TTask):
    t_param_no_default = parameter[str]


class MyConfig(dbnd.Config):
    mc_p = parameter[int]
    mc_q = parameter.value(73)


class MyConfigTester(dbnd.Task):
    t_output = output.json[object]

    def run(self):
        config.log_current_config(sections=["MyConfig"], as_table=True)
        c = MyConfig()
        logger.warning("_p=%s _q=%s", c.mc_p, c.mc_q)
        self.t_output = [c.mc_p, c.mc_q]


class MyAmbiguousClass(Task):
    pass


class MyAmbiguousClass(Task):  # NOQA
    pass


def non_decorated_func():
    pass


NotAClass = None


class TestCmdline(object):
    def test_cmdline_main_task_cls(self):
        dbnd_run_cmd([TTask.get_task_family(), "-r", "t_param=100"])

    def test_cmdline_ambiguous_class(self):
        pytest.raises(TaskClassAmbigiousException, dbnd_run_cmd, ["MyAmbiguousClass"])

    def test_non_existent_class(self):
        with pytest.raises(TaskClassNotFoundException):
            dbnd_run_cmd(["XYZ"])

    def test_non_decorated_class(self):
        dbnd_run_cmd(["test_dbnd.orchestration.test_cmdline.non_decorated_func"])

    def test_not_a_class(self):
        with pytest.raises(TaskClassNotFoundException):
            dbnd_run_cmd(["NotAClass"])

    def test_no_task(self):
        assert dbnd_run_cmd([]) is None

    def test_help(self):
        assert dbnd_run_cmd(["--help"]) is None

    def test_describe_verbose(self):
        args = ["FooBaseTask", "-r", "t_param=hello", "--verbose", "--describe"]
        dbnd_run_cmd(args)

    def test_describe_double_verbose(self):
        args = [
            "FooBaseTask",
            "-r",
            "t_param=hello",
            "--verbose",
            "--verbose",
            "--describe",
        ]
        dbnd_run_cmd(args)

    def test_misspelled_task_suggestion(self):
        with pytest.raises(
            TaskClassNotFoundException, match="dbnd_sanity_check"
        ) as exc_info:
            dbnd_run_cmd(["dbnd_sanity_che", "-r", "x=5"])

        logger.info("exc_info: %s", exc_info)

    def test_get_editdistance(self):
        assert get_editdistance("dbnd_sanity_che", "dbnd_sanity_check") < 5

    def test_cmd_line(self):
        result = dbnd_run_cmd(["TCmdPipe", "-r x=foo", "-r y=bar"])
        assert result.task.x == "foo"
        assert result.task.y == "bar"

    def test_sub_task(self):
        task = dbnd_run_cmd(
            ["TCmdPipe", "-r", "x=foo", "-r", "y=bar", "-s", "TCmdTask.y=xyz"]
        ).task  # type: TCmdPipe
        assert task.x == "foo"
        assert task.y == "bar"
        t_cmd = task.task_dag.select_by_task_names(["TCmdTask"])[0]
        assert t_cmd.y == "xyz"

    def test_local_params(self):
        class MyTask(TTask):
            param1 = parameter[int]
            param2 = parameter.value(default=False)

            def run(self):
                super(MyTask, self).run()
                assert self.param1 == 1 and self.param2

        assert dbnd_run_cmd("MyTask -r param1=1 -r param2=True")

    # def test_set_root_and_task(self):
    # WE SHOULD RAISE IN THIS CASE (conflict)
    #     class MyTask(TTask):
    #         param1 = parameter[int]
    #         param2 = parameter.value(False)
    #
    #         def run(self):
    #             super(MyTask, self).run()
    #             assert self.param1 == 1 and self.param2
    #
    #     # set_root is higher priority
    #     assert dbnd_run_cmd(
    #         "MyTask -r param1=2 -s param1=1 -s 'MyTask.param2=False param2=True'"
    #     )

    def test_specific_takes_precedence(self):
        class MyTask(TTask):
            param = parameter[int]

            def run(self):
                super(MyTask, self).run()
                assert self.param == 6

        assert dbnd_run_cmd("MyTask -r param=5 -o MyTask.param=6")

    def test_cli_with_defaults(self):
        """
        Verify that we also read from the config when we build tasks from the
        command line parsers.
        """

        dbnd_run_cmd(["TConfigTask", "--set", "TConfigTask.t_param=123"])

    def test_cli_no_default(self):
        """
        Verify that we also read from the config when we build tasks from the
        command line parsers.
        """

        set_conf = json.dumps({"TConfigNoDefault": {"t_param_no_default": "123"}})
        dbnd_run_cmd(["TConfigNoDefault", "--set", set_conf])

    def test_cli_raises1(self):
        """
        Verify that we also read from the config when we build tasks from the
        command line parsers.
        """

        dbnd_run_cmd(
            [
                "TConfigTask",
                "-s",
                "TConfigTask.t_param=124",
                "-s",
                "TConfigTask.t_param=123",
            ]
        )
        run_locally__raises(
            DatabandRunError,
            [
                "TConfigTask",
                "-s",
                "TConfigTask.t_param=123",
                "-s",
                "TConfigTask.t_param=124",
            ],
        )

    def test_cli_raises2(self):
        """
        Verify that we also read from the config when we build tasks from the
        command line parsers.
        """

        run_locally__raises(
            DatabandRunError,
            [
                "TConfigTask",
                "-r",
                "p_not_global=124",
                "--set",
                json.dumps({"TConfigTask": {"p_not_global": "123"}}),
            ],
        )

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
