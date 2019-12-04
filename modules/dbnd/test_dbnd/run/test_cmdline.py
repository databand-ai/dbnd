from __future__ import print_function

import logging

import pytest

from dbnd import Task
from dbnd._core.errors import TaskClassAmbigiousException, TaskClassNotFoundException
from dbnd._core.inline import run_cmd_locally
from dbnd._vendor.snippets.edit_distance import get_editdistance
from test_dbnd.factories import TTask


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
        run_cmd_locally([TTask.get_task_family(), "-r", "t_param=100"])

    def test_cmdline_ambiguous_class(self):
        pytest.raises(
            TaskClassAmbigiousException, run_cmd_locally, ["MyAmbiguousClass"]
        )

    def test_non_existent_class(self):
        with pytest.raises(TaskClassNotFoundException):
            run_cmd_locally(["XYZ"])

    def test_non_decorated_class(self):
        run_cmd_locally(["test_dbnd.run.test_cmdline.non_decorated_func"])

    def test_not_a_class(self):
        with pytest.raises(TaskClassNotFoundException):
            run_cmd_locally(["NotAClass"])

    def test_no_task(self):
        assert run_cmd_locally([]) is None

    def test_help(self):
        assert run_cmd_locally(["--help"]) is None

    def test_describe_verbose(self):
        args = ["FooBaseTask", "-r", "t_param=hello", "--verbose", "--describe"]
        assert run_cmd_locally(args)

    def test_misspelled_task_suggestion(self):
        with pytest.raises(
            TaskClassNotFoundException, match="dbnd_sanity_check"
        ) as exc_info:
            run_cmd_locally(["dbnd_sanity_che", "-r", "x=5"])

        logger.info("exc_info: %s", exc_info)

    def test_get_editdistance(self):
        assert get_editdistance("dbnd_sanity_che", "dbnd_sanity_check") < 5
