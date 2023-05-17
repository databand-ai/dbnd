# © Copyright Databand.ai, an IBM Company 2022

import pytest
import six

from mock import Mock

from dbnd._core.constants import MetricSource
from dbnd._core.task_run.task_run_meta_files import TaskRunMetaFiles
from dbnd._core.task_run.task_run_tracker import TaskRunTracker
from dbnd.orchestration.orchestration_tracking.backends.tracking_store_file import (
    FileTrackingStore,
    TaskRunMetricsFileStoreReader,
)
from targets import target
from targets.value_meta import ValueMetaConf


def get_task_run_and_tracker(tmpdir):
    metrics_folder = target(str(tmpdir))

    task_run = Mock()
    task_run.task_run_executor = Mock()
    task_run.task_run_executor.meta_files = TaskRunMetaFiles(metrics_folder)
    t = FileTrackingStore()
    tr_tracker = TaskRunTracker(task_run=task_run, tracking_store=t)
    tr_tracker.settings.tracking.get_value_meta_conf = Mock(
        return_value=ValueMetaConf.enabled()
    )
    return task_run, tr_tracker, metrics_folder


class TestFileMetricsStore(object):
    def test_task_metrics_simple(self, tmpdir, pandas_data_frame):
        task_run, tr_tracker, metrics_folder = get_task_run_and_tracker(tmpdir)

        tr_tracker.log_metric("a", 1)
        tr_tracker.log_metric("a_string", "1")
        tr_tracker.log_metric("a_list", [1, 3])
        tr_tracker.log_metric("a_tuple", (1, 2))

        user_metrics = TaskRunMetricsFileStoreReader(
            metrics_folder
        ).get_all_metrics_values(MetricSource.user)

        assert user_metrics == {
            "a": 1.0,
            "a_list": [1, 3],
            "a_string": 1.0,
            "a_tuple": [1, 2],
        }

    @pytest.mark.skipif(six.PY2, reason="float representation issue with stats.std")
    def test_task_metrics_histograms(self, tmpdir, pandas_data_frame):
        task_run, tr_tracker, metrics_folder = get_task_run_and_tracker(tmpdir)

        tr_tracker.log_data("df", pandas_data_frame, meta_conf=ValueMetaConf.enabled())

        hist_metrics = TaskRunMetricsFileStoreReader(
            metrics_folder
        ).get_all_metrics_values(MetricSource.histograms)

        expected_preview = (
            "   Names  Births  Married\n"
            "     Bob     968     True\n"
            " Jessica     155    False\n"
            "    Mary      77     True\n"
            "    John     578    False\n"
            "     Mel     973     True"
        )

        # std value varies in different py versions due to float precision fluctuation
        df_births_std = hist_metrics["df.Births.std"]
        assert df_births_std == pytest.approx(428.4246)
