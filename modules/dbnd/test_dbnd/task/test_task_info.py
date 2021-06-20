import datetime
import logging
import shlex
import sys

from typing import List

from dbnd import dbnd_run_cmd, parameter, task
from dbnd._core.task_ctrl.task_visualiser import TaskVisualiser
from dbnd_test_scenarios.test_common.task.factories import TTask
from targets.values import DateValueType


logger = logging.getLogger(__name__)


@task
def t_very_long_params(t_param="long_string" * 1000):
    return "ok"


class TaskInfoParamsTask(TTask):
    str_param = parameter[str]
    num_param = parameter[int]
    list_param = parameter[List[int]]
    date_param = parameter.value(DateValueType().parse_from_str("2015-04-03"))
    false_param = parameter.value(False)
    true_param = parameter.value(True)

    def run(self):
        super(TaskInfoParamsTask, self).run()
        assert self.str_param == "15"
        assert self.num_param == 12
        assert self.list_param == [1, 2, 3]
        assert self.date_param == datetime.date(2015, 4, 3)
        assert not self.false_param
        assert self.true_param


class TestTaskInfo(object):
    def test_generated_command_line(self):
        t = TaskInfoParamsTask(str_param=15, num_param=12, list_param=[1, 2, 3])
        cmd_line_as_str = t.ctrl.task_repr.task_command_line
        cmd_line = shlex.split(cmd_line_as_str)

        assert cmd_line_as_str.startswith("dbnd run")
        # check that outputs are filtered out
        assert "t_output" not in cmd_line_as_str
        # check that defaults are filtered out
        assert "date-param" not in cmd_line_as_str

        assert dbnd_run_cmd(cmd_line[2:])

    def test_generated_function_call(self):

        t = TaskInfoParamsTask(str_param=15, num_param=12, list_param=[1, 2, 3])
        func_call = t.ctrl.task_repr.task_functional_call
        logger.info("Func all : %s", func_call)
        task_run = eval(func_call)
        assert task_run.task == t

    def test_simple_dump(self):
        s = TTask(t_param="my_param")
        actual = TaskVisualiser(s).banner("Runinng task")
        assert "my_param" in actual

    def test_exception(self):
        s = TTask(t_param="my_param")
        try:
            raise Exception("MyException")
        except Exception:
            actual = TaskVisualiser(s).banner("Runinng task", exc_info=sys.exc_info())
            assert actual
            assert "MyException" in actual

    def test_in_memory_dump(self):
        s = t_very_long_params.task(t_param="long_string" * 1000)
        assert len(s.t_param) > s.settings.describe.console_value_preview_size * 3

        actual = TaskVisualiser(s).banner("Running task")
        logger.warning(actual)
        assert len(actual) < s.settings.describe.console_value_preview_size * 3
