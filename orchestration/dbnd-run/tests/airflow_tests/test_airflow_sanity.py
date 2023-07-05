# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd import dbnd_run_cmd
from dbnd_run.tasks.basics import dbnd_sanity_check
from dbnd_run.testing.helpers import assert_run_task


logger = logging.getLogger(__name__)


class TestAirflowSanityTasks(object):
    def test_sanity_check_task(self):
        check_task = dbnd_sanity_check.task()
        assert_run_task(check_task)

    def test_sanity_check_cmd(self):
        dbnd_run_cmd(["dbnd_sanity_check"])
