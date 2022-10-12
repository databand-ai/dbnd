# © Copyright Databand.ai, an IBM Company 2022

from mock.mock import Mock, patch

from dbnd._core.task_run.task_run_tracker import TaskRunTracker
from targets.value_meta import ValueMeta


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

    @patch("dbnd._core.task_run.task_run_tracker.log_exception")
    @patch("dbnd._core.task_run.task_run_tracker.get_value_meta")
    def test_calc_meta_data_logs_exception(
        self, get_value_meta: Mock, log_exception: Mock
    ):
        """
        In case of error, empty ValueMeta will be returned.
        """
        get_value_meta.side_effect = Exception("Fake error")
        tracker = TaskRunTracker(Mock(), {})
        actual = tracker._calc_meta_data(Mock(), Mock())
        expected = ValueMeta("")
        assert actual == expected
        log_exception.assert_called()
