from dbnd import dbnd_run_cmd
from dbnd._core.constants import TaskRunState


class TestSpecificTaskRun(object):
    def test_run_selected_task(self):
        result_run = dbnd_run_cmd(
            [
                "dbnd_test_scenarios.pipelines.simple_pipeline.simple_pipeline",
                "--set",
                "run.selected_tasks_regex=log_some_data",
            ]
        )
        task_runs_dict = {
            tr.task.task_name: tr.task_run_state for tr in result_run.task_runs
        }
        assert task_runs_dict["dbnd_driver"] == TaskRunState.SUCCESS
        assert task_runs_dict["get_some_data"] == TaskRunState.SUCCESS
        assert task_runs_dict["log_some_data"] == TaskRunState.SUCCESS
        assert task_runs_dict["calc_and_log"] is None
        assert (
            task_runs_dict[
                "dbnd_test_scenarios.pipelines.simple_pipeline.simple_pipeline"
            ]
            is None
        )
