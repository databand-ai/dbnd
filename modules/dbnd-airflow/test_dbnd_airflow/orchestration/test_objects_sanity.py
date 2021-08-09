from dbnd import task
from dbnd.tasks.basics import SimplestTask
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase


class TestTaskObjectSanity(TargetTestBase):
    def test_serialization_in_airflow(self):
        @task
        def simple_task_with_serialization():
            run = SimplestTask().dbnd_run()
            run.run_executor.save_run_pickle(self.target("t.pickle"))

        simple_task_with_serialization.dbnd_run()
