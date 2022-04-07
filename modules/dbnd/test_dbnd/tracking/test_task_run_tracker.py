from mock.mock import Mock

from dbnd._core.task_run.task_run_tracker import TaskRunTracker


class TestTaskRunTracker:
    def test_task_run_url(self):
        stub_task_run = Mock()
        stub_task_run.run.tracker.databand_url = "http://example.com"
        stub_task_run.run.job_name = "test_job"
        stub_task_run.run.run_uid = "00000000-0000-0000-0000-000000000001"
        stub_task_run.task_run_uid = "00000000-0000-0000-0000-000000000002"
        tracker = TaskRunTracker(stub_task_run, {})
        expected = "http://example.com/app/jobs/test_job/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002"
        assert tracker.task_run_url() == expected
