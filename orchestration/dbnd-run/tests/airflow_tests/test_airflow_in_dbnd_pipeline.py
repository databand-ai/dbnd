# © Copyright Databand.ai, an IBM Company 2022

from airflow.operators.bash_operator import BashOperator

from dbnd import Task, pipeline
from dbnd.testing.helpers_pytest import skip_on_windows
from dbnd_run.testing.helpers import TTask


@skip_on_windows
class TestLegacyAirflowInplace(object):
    def test_inline_airflow_operators_with_class(self):
        class TInlineAirflowOpsPipeline(Task):
            def band(self):
                t2 = BashOperator(task_id="sleep1", bash_command="sleep 0.1", retries=3)
                self.set_upstream(t2)

        TInlineAirflowOpsPipeline().dbnd_run()

    def test_inline_airflow_operators_with_func(self):
        @pipeline
        def pipline_that_has_airflow():
            t1 = TTask()
            t2 = BashOperator(task_id="sleep2", bash_command="sleep 0.1", retries=3)
            t1.set_upstream(t2)
            return t1

        pipline_that_has_airflow.dbnd_run()
