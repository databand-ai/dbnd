# © Copyright Databand.ai, an IBM Company 2022

from dbnd import task
from dbnd.testing.orchestration_utils import TargetTestBase
from dbnd_run.tasks.basics import SimplestTask


class TestTaskObjectSanity(TargetTestBase):
    def test_serialization_in_airflow(self):
        @task
        def simple_task_with_serialization():
            run = SimplestTask().dbnd_run()
            run.run_executor.save_run_pickle(self.target("t.pickle"))

        simple_task_with_serialization.dbnd_run()
