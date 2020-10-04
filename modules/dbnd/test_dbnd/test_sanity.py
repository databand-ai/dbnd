import logging

from dbnd import dbnd_run_cmd
from dbnd.tasks.basics import dbnd_sanity_check
from dbnd.testing.helpers_pytest import assert_run_task


logger = logging.getLogger(__name__)


class TestSanityTasks(object):
    def test_sanity_check_task(self):
        check_task = dbnd_sanity_check.task()
        assert_run_task(check_task)

    def test_sanity_check_cmd(self):
        dbnd_run_cmd(["dbnd_sanity_check"])
