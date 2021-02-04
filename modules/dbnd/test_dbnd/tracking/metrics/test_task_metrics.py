import logging
import os

from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.task.factories import TTask


logger = logging.getLogger(__name__)


class TestTaskMetrics(object):
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
