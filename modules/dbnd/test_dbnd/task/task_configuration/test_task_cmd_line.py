import json
import logging

from dbnd import PythonTask, parameter
from dbnd._core.errors import DatabandExecutorError
from dbnd._core.inline import run_cmd_locally, run_cmd_locally_split
from dbnd.testing.helpers_pytest import run_locally__raises
from test_dbnd.factories import TTask


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


class TestTaskCmdLine(object):
    def test_cmd_line(self):
        result = run_cmd_locally(["TCmdPipe", "-r x=foo", "-r y=bar"])
        assert result.task.x == "foo"
        assert result.task.y == "bar"

    def test_sub_task(self):
        task = run_cmd_locally(
            ["TCmdPipe", "-r", "x=foo", "-r", "y=bar", "-s", "TCmdTask.y=xyz"]
        ).task  # type: TCmdPipe
        assert task.x == "foo"
        assert task.y == "bar"
        t_cmd = task.task_dag.select_by_task_names("TCmdTask")[0]
        assert t_cmd.y == "xyz"

    def test_local_params(self):
        class MyTask(TTask):
            param1 = parameter[int]
            param2 = parameter.value(default=False)

            def run(self):
                super(MyTask, self).run()
                assert self.param1 == 1 and self.param2

        assert run_cmd_locally_split("MyTask -r param1=1 -r param2=True")

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
    #     assert run_cmd_locally_split(
    #         "MyTask -r param1=2 -s param1=1 -s 'MyTask.param2=False param2=True'"
    #     )

    def test_specific_takes_precedence(self):
        class MyTask(TTask):
            param = parameter[int]

            def run(self):
                super(MyTask, self).run()
                assert self.param == 6

        assert run_cmd_locally_split("MyTask -r param=5 -o MyTask.param=6")

    def test_cli_with_defaults(self):
        """
        Verify that we also read from the config when we build tasks from the
        command line parsers.
        """

        run_cmd_locally(["TConfigTask", "--set", "TConfigTask.t_param=123"])

    def test_cli_no_default(self):
        """
        Verify that we also read from the config when we build tasks from the
        command line parsers.
        """

        set_conf = json.dumps({"TConfigNoDefault": {"t_param_no_default": "123"}})
        run_cmd_locally(["TConfigNoDefault", "--set", set_conf])

    def test_cli_raises1(self):
        """
        Verify that we also read from the config when we build tasks from the
        command line parsers.
        """

        run_cmd_locally(
            [
                "TConfigTask",
                "-s",
                "TConfigTask.t_param=124",
                "-s",
                "TConfigTask.t_param=123",
            ]
        )
        run_locally__raises(
            DatabandExecutorError,
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
            DatabandExecutorError,
            [
                "TConfigTask",
                "-r",
                "p_not_global=124",
                "--set",
                json.dumps({"TConfigTask": {"p_not_global": "123"}}),
            ],
        )
