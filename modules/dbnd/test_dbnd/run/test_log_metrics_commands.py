import os

from typing import Dict

from dbnd import as_task, band, task
from dbnd._core.commands import log_artifact, log_metric
from dbnd._core.current import get_databand_run
from dbnd._core.tracking.backends.tracking_store_file import read_task_metrics
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase


class TestTaskMetricsCommands(TargetTestBase):
    def test_log_metric(self):
        @task
        def t_f_metric(a=5):
            log_metric("t_f", a)

        t = assert_run_task(t_f_metric.t())
        assert (
            t.ctrl.last_task_run.meta_files.get_metric_target("t_f").read().split()[1]
            == "5"
        )

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

    def test_log__write_read_metrics(self, tmpdir):
        @task
        def write_metrics(a=5):
            log_metric("t_f", a)

        @task
        def read_metrics(metrics_task_id):
            # type: ( str) -> Dict
            source_task_attempt_folder = (
                get_databand_run().get_task_run(metrics_task_id).attempt_folder
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
