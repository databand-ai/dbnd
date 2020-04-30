from __future__ import print_function

import logging

import pytest

from dbnd import Task, dbnd_run_cmd
from dbnd._core.errors import TaskClassAmbigiousException, TaskClassNotFoundException
from dbnd._vendor.snippets.edit_distance import get_editdistance
from dbnd_test_scenarios.test_common.task.factories import TTask


logger = logging.getLogger(__name__)


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
        dbnd_run_cmd(["test_dbnd.run.test_cmdline.non_decorated_func"])

    def test_not_a_class(self):
        with pytest.raises(TaskClassNotFoundException):
            dbnd_run_cmd(["NotAClass"])

    def test_no_task(self):
        assert dbnd_run_cmd([]) is None

    def test_help(self):
        assert dbnd_run_cmd(["--help"]) is None

    def test_describe_verbose(self):
        args = ["FooBaseTask", "-r", "t_param=hello", "--verbose", "--describe"]
        assert dbnd_run_cmd(args)

    def test_describe_double_verbose(self):
        args = [
            "FooBaseTask",
            "-r",
            "t_param=hello",
            "--verbose",
            "--verbose",
            "--describe",
        ]
        assert dbnd_run_cmd(args)

    def test_misspelled_task_suggestion(self):
        with pytest.raises(
            TaskClassNotFoundException, match="dbnd_sanity_check"
        ) as exc_info:
            dbnd_run_cmd(["dbnd_sanity_che", "-r", "x=5"])

        logger.info("exc_info: %s", exc_info)

    def test_get_editdistance(self):
        assert get_editdistance("dbnd_sanity_che", "dbnd_sanity_check") < 5
