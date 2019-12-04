from datetime import datetime, timedelta

from airflow.operators.bash_operator import BashOperator

from dbnd import Task, dbnd_run_cmd, run_task
from dbnd.testing.helpers import (
    run_dbnd_subprocess__dbnd,
    run_dbnd_subprocess__dbnd_run,
)
from dbnd.testing.helpers_pytest import skip_on_windows


@skip_on_windows
class TestLegacyAirflowInplace(object):
    def test_run_airflow_dag(self):
        """
        Test that `databand --help` fits on one screen
        """
        dbnd_run_cmd(["bash_dag"])

    def test_inline_airflow_operators(self):
        class TInlineAirflowOpsPipeline(Task):
            def band(self):
                t2 = BashOperator(task_id="sleep", bash_command="sleep 0.1", retries=3)
                self.set_upstream(t2)

        run_task(TInlineAirflowOpsPipeline())


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
        run_dbnd_subprocess__dbnd(
            [
                "airflow",
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
        run_dbnd_subprocess__dbnd(
            [
                "airflow",
                "run",
                "bash_dag",
                "print_date",
                datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "--local",
                "-i",
            ]
        )
