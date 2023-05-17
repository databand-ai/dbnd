# © Copyright Databand.ai, an IBM Company 2022

import logging
import os

from typing import Dict

import pytest

from dbnd import as_task, band, dbnd_config, log_artifact, log_metric, task
from dbnd._core.context.databand_context import DatabandContext
from dbnd._core.current import get_databand_run
from dbnd._vendor.pendulum import utcnow
from dbnd.orchestration.orchestration_tracking.backends.tracking_store_file import (
    read_task_metrics,
)
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd.testing.orchestration_utils import TargetTestBase
from dbnd_test_scenarios.test_common.task.factories import TTask


logger = logging.getLogger(__name__)


@pytest.fixture
def matplot_figure():
    import matplotlib.pyplot as plt
    import numpy as np

    fig = plt.figure()
    ax1 = fig.add_subplot(2, 2, 1)
    ax1.hist(np.random.randn(100), bins=20, alpha=0.3)
    ax2 = fig.add_subplot(2, 2, 2)
    ax2.scatter(np.arange(30), np.arange(30) + 3 * np.random.randn(30))
    fig.add_subplot(2, 2, 3)
    return fig


class TestTaskMetricsCommands(TargetTestBase):
    def test_log_metric(self):
        @task
        def t_f_metric(a=5):
            log_metric("t_f", a)

        t = assert_run_task(t_f_metric.t())
        assert (
            t.ctrl.last_task_run.task_run_executor.meta_files.get_metric_target("t_f")
            .read()
            .split()[1]
            == "5"
        )

    def test_log_metric_pendulum(self):
        now = utcnow()

        @task
        def t_f_metric():
            log_metric("t_f", now)

        with dbnd_config({"core": {"tracker": ["file", "console"]}}):
            with DatabandContext.new_context(allow_override=True):
                t = assert_run_task(t_f_metric.t())
                task_run_executor = t.ctrl.last_task_run.task_run_executor
                task_run_executor.meta_files.get_metric_target("t_f").read()
                assert task_run_executor.meta_files.get_metric_target(
                    "t_f"
                ).read().split()[1] == str(now)

    def test_log__write_read_metrics(self, tmpdir):
        @task
        def write_metrics(a=5):
            log_metric("t_f", a)

        @task
        def read_metrics(metrics_task_id):
            # type: ( str) -> Dict
            source_task_attempt_folder = (
                get_databand_run()
                .get_task_run(metrics_task_id)
                .task_run_executor.attempt_folder
            )
            metrics = read_task_metrics(source_task_attempt_folder)
            return metrics

        @band
        def metrics_flow():
            w = write_metrics()
            r = read_metrics(metrics_task_id=w.task.task_id)
            as_task(r).set_upstream(w)

            return r

        t = assert_run_task(metrics_flow.t())

        metrics = t.result.load(value_type=Dict)

        assert {"t_f": 5} == metrics

    def test_task_metrics_simple(self, pandas_data_frame):
        class TTaskMetrics(TTask):
            def run(self):
                self.metrics.log_metric("inner", 3)
                self.log_metric("a", 1)
                self.log_metric("a_string", "1")
                self.log_metric("a_list", [1, 3])
                self.log_metric("a_tuple", (1, 2))
                self.log_dataframe("df", pandas_data_frame)
                super(TTaskMetrics, self).run()

        task = TTaskMetrics()
        assert_run_task(task)
        actual = task._meta_output.list_partitions()
        actuals_strings = list(map(str, actual))
        assert any(["inner" in s for s in actuals_strings])
        assert any(["a_string" in s for s in actuals_strings])
        assert any(["a_list" in s for s in actuals_strings])
        assert any(["a_tuple" in s for s in actuals_strings])
        assert any(["df.schema" in s for s in actuals_strings])
        assert any(["df.shape0" in s for s in actuals_strings])
        assert any(["df.shape1" in s for s in actuals_strings])

    def test_task_artifacts(self, matplot_figure, tmpdir):
        lorem = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt\n"
        data = tmpdir / "data.txt"
        data.write(lorem)

        artifact_dir = tmpdir.mkdir("dir")
        sub_file = artifact_dir.mkdir("subdir").join("sub_file")
        sub_file.write(lorem)

        class TTaskArtifacts(TTask):
            def run(self):
                self.log_artifact("my_tmp_file", str(data))
                self.log_artifact("my_figure", matplot_figure)
                self.log_artifact("my_dir", str(artifact_dir) + "/")
                super(TTaskArtifacts, self).run()

        task = TTaskArtifacts()
        assert_run_task(task)
        actual = task._meta_output.list_partitions()
        actual_strings = list(map(str, actual))
        assert any(["my_tmp_file" in os.path.basename(s) for s in actual_strings])
        assert any(["my_figure" in os.path.basename(s) for s in actual_strings])
        assert any(["sub_file" in os.path.basename(s) for s in actual_strings])

    def test_log_artifact(self, tmpdir):
        lorem = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt\n"
        f = tmpdir.join("abcd")
        f.write(lorem)

        @task
        def t_f_artifact(a=5):
            log_artifact("t_a", str(f))

        t = assert_run_task(t_f_artifact.t())
        actual = t._meta_output.list_partitions()
        actual_strings = list(map(str, actual))
        assert any(["t_a" in os.path.basename(s) for s in actual_strings])
