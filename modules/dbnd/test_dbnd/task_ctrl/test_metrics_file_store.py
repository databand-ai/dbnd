import pytest
import six

from mock import Mock

from dbnd._core.constants import MetricSource
from dbnd._core.task_run.task_run_meta_files import TaskRunMetaFiles
from dbnd._core.task_run.task_run_tracker import TaskRunTracker
from dbnd._core.tracking.backends.tracking_store_file import (
    FileTrackingStore,
    TaskRunMetricsFileStoreReader,
)
from dbnd._core.tracking.histograms import HistogramRequest
from targets import target
from targets.value_meta import ValueMetaConf


class TestFileMetricsStore(object):
    def test_task_metrics_simple(self, tmpdir, pandas_data_frame):
        metrics_folder = target(str(tmpdir))

        task_run = Mock()
        task_run.meta_files = TaskRunMetaFiles(metrics_folder)
        t = FileTrackingStore()
        tr_tracker = TaskRunTracker(task_run=task_run, tracking_store=t)
        tr_tracker.settings.features.get_value_meta_conf = Mock(
            return_value=ValueMetaConf.enabled()
        )
        tr_tracker.log_metric("a", 1)
        tr_tracker.log_metric("a_string", "1")
        tr_tracker.log_metric("a_list", [1, 3])
        tr_tracker.log_metric("a_tuple", (1, 2))

        user_metrics = TaskRunMetricsFileStoreReader(
            metrics_folder
        ).get_all_metrics_values(MetricSource.user)

        assert user_metrics == {
            "a": 1.0,
            "a_list": "[1, 3]",
            "a_string": 1.0,
            "a_tuple": "(1, 2)",
        }

    @pytest.mark.skipif(six.PY2, reason="float representation issue with stats.std")
    def test_task_metrics_histograms(self, tmpdir, pandas_data_frame):
        metrics_folder = target(str(tmpdir))

        task_run = Mock()
        task_run.meta_files = TaskRunMetaFiles(metrics_folder)
        t = FileTrackingStore()
        tr_tracker = TaskRunTracker(task_run=task_run, tracking_store=t)
        tr_tracker.settings.features.get_value_meta_conf = Mock(
            return_value=ValueMetaConf.enabled()
        )
        tr_tracker.log_data(
            "df", pandas_data_frame, meta_conf=ValueMetaConf.enabled(),
        )

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
        hist_metrics["df.histograms"].pop("Names")
        assert hist_metrics == {
            "df.Births.type": "int64",
            "df.Births.25%": 155.0,
            "df.Births.50%": 578.0,
            "df.Births.75%": 968.0,
            "df.Births.count": 5.0,
            "df.Births.distinct": 5,
            "df.Births.std": df_births_std,
            "df.Births.max": 973.0,
            "df.Births.mean": 550.2,
            "df.Births.min": 77.0,
            "df.Births.non-null": 5,
            "df.Births.null-count": 0,
            "df.Married.count": 5,
            "df.Married.distinct": 2,
            "df.Married.freq": 3,
            "df.Married.non-null": 5,
            "df.Married.null-count": 0,
            "df.Married.type": "bool",
            "df.Married.unique": 2,
            "df.Names.count": 5,
            "df.Names.distinct": 5,
            "df.Names.freq": 1,
            "df.Names.non-null": 5,
            "df.Names.null-count": 0,
            "df.Names.type": "object",
            "df.Names.unique": 5,
            "df.histograms": {
                "Births": [[2, 0, 1, 2], [77.0, 301.0, 525.0, 749.0, 973.0]],
                "Married": [[3, 2], [True, False]],
            },
            "df.preview": expected_preview,
            "df.schema": {
                "columns": ["Names", "Births", "Married"],
                "dtypes": {"Births": "int64", "Names": "object", "Married": "bool"},
                "shape": [5, 3],
                "size": 15,
                "type": "DataFrame",
            },
            "df.shape": [5, 3],
            "df.shape0": 5,
            "df.shape1": 3,
            "df.stats": {
                "Births": {
                    "type": "int64",
                    "25%": 155.0,
                    "50%": 578.0,
                    "75%": 968.0,
                    "count": 5.0,
                    "distinct": 5,
                    "max": 973.0,
                    "mean": 550.2,
                    "min": 77.0,
                    "non-null": 5,
                    "null-count": 0,
                    "std": df_births_std,
                },
                "Married": {
                    "count": 5,
                    "distinct": 2,
                    "freq": 3,
                    "non-null": 5,
                    "null-count": 0,
                    "type": "bool",
                    "unique": 2,
                },
                "Names": {
                    "count": 5,
                    "distinct": 5,
                    "freq": 1,
                    "non-null": 5,
                    "null-count": 0,
                    "type": "object",
                    "unique": 5,
                },
            },
        }
