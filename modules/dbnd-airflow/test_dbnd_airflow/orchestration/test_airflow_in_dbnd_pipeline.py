from airflow.operators.bash_operator import BashOperator

from dbnd import Task, dbnd_run_cmd, pipeline
from dbnd.testing.helpers_pytest import skip_on_windows
from dbnd_test_scenarios.test_common.task.factories import TTask


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

        TInlineAirflowOpsPipeline().dbnd_run()

    def test_inline_airflow_operators(self):
        @pipeline
        def pipline_that_has_airflow():
            t1 = TTask()
            t2 = BashOperator(task_id="sleep", bash_command="sleep 0.1", retries=3)
            t1.set_upstream(t2)
            return t1

        pipline_that_has_airflow.dbnd_run()
