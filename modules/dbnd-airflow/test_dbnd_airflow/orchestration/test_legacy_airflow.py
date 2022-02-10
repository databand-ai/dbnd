from datetime import datetime, timedelta

import pytest

from dbnd.testing.helpers import run_dbnd_subprocess__dbnd_run, run_subprocess__airflow
from dbnd.testing.helpers_pytest import skip_on_windows
from dbnd_airflow.constants import AIRFLOW_VERSION_2


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
        additional = []
        if AIRFLOW_VERSION_2:
            additional = ["dags"]
        run_subprocess__airflow(
            additional
            + [
                "backfill",
                "bash_dag",
                "-s",
                (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            ]
        )

    # TODO Airflow 2.2
    @pytest.mark.skipif(AIRFLOW_VERSION_2, reason="Needs investigation")
    def test_run_task(self):
        """
        CLI COMMAND:
             airflow tasks run bash_dag print_date 2021-12-19T14:09:30 --local -i

        Error:
             airflow.exceptions.DagRunNotFound: DagRun for bash_dag with run_id or execution_date of '2021-12-21T17:24:30' not found
        """
        additional = []
        if AIRFLOW_VERSION_2:
            additional = ["tasks"]
        run_subprocess__airflow(
            additional
            + [
                "run",
                "bash_dag",
                "print_date",
                datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "--local",
                "-i",
            ]
        )
