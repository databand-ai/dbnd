from dbnd.testing.helpers import run_dbnd_subprocess__dbnd_run
from test_dbnd.run.utils import DbndCmdTest


class TestResubmit(DbndCmdTest):
    def test_external_task_cmd_line(self):
        run_dbnd_subprocess__dbnd_run(["dbnd_sanity_check", "--env", "local_resubmit"])

    def test_submit_can_run(self):
        self.dbnd_run_task_with_output(["--env", "local_resubmit"])

    def test_submit_with_interactive(self):
        self.dbnd_run_task_with_output(["--env", "local_resubmit", "--interactive"])

    def test_submit_with_local_driver(self):
        self.dbnd_run_task_with_output(["--env", "local_resubmit", "--local-driver"])
