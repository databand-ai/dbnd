from dbnd.testing.helpers import run_dbnd_subprocess__dbnd_run
from dbnd.testing.helpers_pytest import skip_on_windows
from test_dbnd.run.utils import DbndCmdTest


class TestResubmit(DbndCmdTest):
    @skip_on_windows
    def test_external_task_cmd_line(self):
        run_dbnd_subprocess__dbnd_run(["dbnd_sanity_check", "--env", "local_resubmit"])

    @skip_on_windows
    def test_submit_can_run(self):
        self.dbnd_run_task_with_output(["--env", "local_resubmit"])

    @skip_on_windows
    def test_submit_with_interactive(self):
        self.dbnd_run_task_with_output(["--env", "local_resubmit", "--interactive"])

    def test_submit_with_local_driver(self):
        self.dbnd_run_task_with_output(["--env", "local_resubmit", "--local-driver"])
