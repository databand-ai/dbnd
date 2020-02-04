from datetime import datetime, timedelta

from dbnd.testing.helpers import run_dbnd_subprocess__dbnd_run, run_subprocess__airflow
from dbnd.testing.helpers_pytest import skip_on_windows


@skip_on_windows
class TestLegacyAirflowIntegration(object):
    def test_run_airflow_dag(self):
        """
        Test that `databand --help` fits on one screen
        """
        run_dbnd_subprocess__dbnd_run(["bash_dag"])

    def test_backfill_airflow_dag(self):
        """
        Test that `databand --help` fits on one screen
        """
        run_subprocess__airflow(
            [
                "backfill",
                "bash_dag",
                "-s",
                (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            ]
        )

    def test_run_task(self):
        """
        Test that `databand --help` fits on one screen
        """
        run_subprocess__airflow(
            [
                "run",
                "bash_dag",
                "print_date",
                datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "--local",
                "-i",
            ]
        )
