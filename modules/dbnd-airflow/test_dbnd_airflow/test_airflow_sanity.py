import logging

from dbnd._core.inline import run_cmd_locally
from dbnd.tasks.basics import dbnd_sanity_check
from dbnd.testing import assert_run_task


logger = logging.getLogger(__name__)


class TestAirflowSanityTasks(object):
    def test_sanity_check_task(self):
        check_task = dbnd_sanity_check.task()
        assert_run_task(check_task)

    def test_sanity_check_cmd(self):
        run_cmd_locally(["dbnd_sanity_check"])
