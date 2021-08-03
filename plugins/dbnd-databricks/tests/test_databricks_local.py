from __future__ import absolute_import

import pytest

from airflow.contrib.hooks.databricks_hook import RunState
from mock import patch

from dbnd_databricks.databricks_config import DatabricksConfig
from dbnd_spark.spark_config import SparkConfig
from dbnd_test_scenarios.spark.spark_tasks import WordCountPySparkTask, WordCountTask


test_config = {
    SparkConfig.jars: None,
    SparkConfig.main_jar: __file__,
    SparkConfig.num_executors: 77,
    DatabricksConfig.cloud_type: "aws",
    DatabricksConfig.status_polling_interval_seconds: 1,
    DatabricksConfig.spark_version: "test",
}


def my_submit_run(*args):
    try:
        output_path = args[0][-1]
    except KeyError:
        output_path = args[0]["spark_submit_task"]["parameters"][-1]
    with open(output_path, "w") as file:
        file.write("test was ok")
    return 66 + len(args)


class TestDatabricks(object):
    @pytest.mark.databricks_mocked
    @patch("airflow.contrib.hooks.databricks_hook.DatabricksHook")
    def test_simple_java(self, d_hook):
        run_states = [
            RunState("RUNNING", "gla", "bla"),
            RunState("TERMINATED", "SUCCESS", "yey"),
        ]
        d_hook_instance = d_hook.return_value
        d_hook_instance.submit_run = my_submit_run
        d_hook_instance.get_run_state.side_effect = run_states
        d_hook_instance.get_run_page_url.return_value = (
            "https://databand.ai-test-databricks"
        )
        t = WordCountTask(
            text=__file__,
            spark_engine="databricks",
            task_version="now",
            override=test_config,
        )
        t.dbnd_run()

    @pytest.mark.databricks_mocked
    @patch("airflow.contrib.hooks.databricks_hook.DatabricksHook")
    def test_simple_python(self, d_hook):
        run_states = [
            RunState("RUNNING", "gla", "bla"),
            RunState("TERMINATED", "SUCCESS", "yey"),
        ]
        d_hook_instance = d_hook.return_value
        d_hook_instance.submit_run = my_submit_run
        d_hook_instance.get_run_state.side_effect = run_states
        d_hook_instance.get_run_page_url.return_value = (
            "https://databand.ai-test-databricks"
        )

        WordCountPySparkTask(
            text=__file__,
            spark_engine="databricks",
            task_version="now",
            override=test_config,
        ).dbnd_run()
