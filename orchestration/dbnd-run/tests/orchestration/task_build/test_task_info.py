# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging
import os
import shlex
import sys

from textwrap import dedent
from typing import List

from mock import patch

from dbnd import dbnd_run_cmd, new_dbnd_context, parameter, task
from dbnd_run.testing.helpers import TTask
from targets import target
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
    none_param = parameter[str]
    str_as_target = parameter[str]

    def run(self):
        super(TaskInfoParamsTask, self).run()
        assert self.str_param == "15"
        assert self.num_param == 12
        assert self.list_param == [1, 2, 3]
        assert self.date_param == datetime.date(2015, 4, 3)
        assert not self.false_param
        assert self.true_param
        assert self.none_param is None  # check for @none support
        assert self.str_as_target is not None  # check for @target support
        assert (
            "THIS FILE" in self.str_as_target
        )  # we load this file, so THIS FILE is there :)


class TestTaskInfo(object):
    def test_generated_command_line(self):
        t = TaskInfoParamsTask(
            str_param=15,
            num_param=12,
            list_param=[1, 2, 3],
            none_param=None,
            str_as_target=target(__file__),
        )
        cmd_line_as_str = t.ctrl.task_repr.task_command_line
        cmd_line = shlex.split(cmd_line_as_str)
        logger.info("Command line: %s", cmd_line_as_str)
        assert cmd_line_as_str.startswith("dbnd run")
        # check that outputs are filtered out
        assert "t_output" not in cmd_line_as_str
        # check that defaults are filtered out
        assert "date-param" not in cmd_line_as_str

        assert dbnd_run_cmd(cmd_line[2:])

    def test_generated_function_call(self):

        t = TaskInfoParamsTask(
            str_param=15,
            num_param=12,
            list_param=[1, 2, 3],
            none_param=None,
            str_as_target=target(__file__),
        )
        func_call = t.ctrl.task_repr.task_functional_call

        import tests

        # the func call will have "full module", so we need ot be able to import it

        logger.info("Func call : %s, imported module: %s", func_call, tests)
        task_run = eval(func_call)
        assert task_run.task == t

    def test_simple_dump(self):
        s = TTask(t_param="my_param")
        actual = s.ctrl.banner("Runinng task")
        assert "my_param" in actual

    def test_exception(self):
        s = TTask(t_param="my_param")
        try:
            raise Exception("MyException")
        except Exception:
            actual = s.ctrl.banner("Running task", exc_info=sys.exc_info())
            assert actual
            assert "MyException" in actual

    def test_in_memory_dump(self):
        s = t_very_long_params.task(t_param="long_string" * 1000)
        assert len(s.t_param) > s.settings.tracking_log.console_value_preview_size * 3

        actual = s.ctrl.banner("Running task")
        logger.warning(actual)
        assert len(actual) < s.settings.tracking_log.console_value_preview_size * 3


class TestParamsBannerPreviewLogs:
    def test_params_preview(self):
        PARAMS_WITH_PREVIEWS = dedent(
            """
            Name        Kind    Type    Format    Source                -= Value =-
            num_param   param   int               t.o.t.t.t_f[default]  12
            list_param  param   List              t.o.t.t.t_f[default]  [1,2,3]
            none_param  param   object            t.o.t.t.t_f[default]  @None
            """
        )

        with new_dbnd_context(conf={"tracking": {"log_value_preview": True}}):

            @task
            def t_f(num_param=12, list_param=[1, 2, 3], none_param=None):
                return "123"

            run = t_f.dbnd_run()
            task_run = run.root_task_run
            actual = task_run.task.ctrl.banner("test")
        logger.info("Banner: %s", actual)
        assert PARAMS_WITH_PREVIEWS in actual
        assert "result.pickle" in actual

    def test_params_without_preview(self):
        PARAMS_WITHOUT_PREVIEWS = dedent(
            """
            Name        Kind    Type    Format    Source                -= Value =-
            num_param   param   int               t.o.t.t.t_f[default]  -
            list_param  param   List              t.o.t.t.t_f[default]  "-"
            none_param  param   object            t.o.t.t.t_f[default]  -
            result      output  object  .pickle                         "-"
            """
        )

        with new_dbnd_context(conf={"tracking": {"log_value_preview": False}}):

            @task
            def t_f(num_param=12, list_param=[1, 2, 3], none_param=None):
                return "123"

            run = t_f.dbnd_run()
            task_run = run.root_task_run
            env = {"DBND__NO_TABLES": "True"}
            with patch.dict(os.environ, env):
                actual = task_run.task.ctrl.banner("test")
                logger.info("Banner: %s", actual)
                assert PARAMS_WITHOUT_PREVIEWS in actual
