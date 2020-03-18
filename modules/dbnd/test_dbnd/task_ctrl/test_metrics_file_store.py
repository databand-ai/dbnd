from mock import Mock

from dbnd._core.task_run.task_run_meta_files import TaskRunMetaFiles
from dbnd._core.task_run.task_run_tracker import TaskRunTracker
from dbnd._core.tracking.tracking_store_file import (
    FileTrackingStore,
    TaskRunMetricsFileStoreReader,
)
from targets import target


class TestFileMetricsStore(object):
    def test_task_metrics_simple(self, tmpdir, pandas_data_frame):
        metrics_folder = target(str(tmpdir))

        task_run = Mock()
        task_run.meta_files = TaskRunMetaFiles(metrics_folder)
        t = FileTrackingStore()
        tr_tracker = TaskRunTracker(task_run=task_run, tracking_store=t)
        tr_tracker.log_metric("a", 1)
        tr_tracker.log_metric("a_string", "1")
        tr_tracker.log_metric("a_list", [1, 3])
        tr_tracker.log_metric("a_tuple", (1, 2))
        tr_tracker.log_dataframe("df", pandas_data_frame)

        actual = TaskRunMetricsFileStoreReader(metrics_folder).get_all_metrics_values()

        print(actual)
        assert actual == {
            "a": 1.0,
            "a_list": "[1, 3]",
            "a_string": 1.0,
            "a_tuple": "(1, 2)",
            "df.preview": "Names  Births",
            "df.schema": "{",
            "df.shape": "[5, 2]",
            "df.shape_0_": 5.0,
            "df.shape_1_": 2.0,
        }
