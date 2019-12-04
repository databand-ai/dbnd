from dbnd import dbnd_run_cmd
from dbnd.testing.helpers import run_dbnd_subprocess__dbnd_run


class TestResubmit(object):
    def test_external_task_cmd_line(self):
        run_dbnd_subprocess__dbnd_run(["dbnd_sanity_check", "--env", "local_resubmit"])

    def test_external_task_inmemory(self):
        dbnd_run_cmd(["dbnd_sanity_check", "--env", "local_resubmit"])
