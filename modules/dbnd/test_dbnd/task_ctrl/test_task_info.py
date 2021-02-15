import datetime
import logging
import shlex

from typing import List

from dbnd import dbnd_run_cmd, parameter
from dbnd_test_scenarios.test_common.task.factories import TTask
from targets.values import DateValueType


logger = logging.getLogger(__name__)


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
        import test_dbnd

        t = TaskInfoParamsTask(str_param=15, num_param=12, list_param=[1, 2, 3])
        func_call = t.ctrl.task_repr.task_functional_call
        logger.info("Func all : %s", func_call)
        task_run = eval(func_call)
        assert task_run.task == t
