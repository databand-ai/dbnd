from __future__ import absolute_import

from airflow.contrib.hooks.databricks_hook import RunState
from mock import patch

from dbnd_spark.spark_config import SparkConfig
from dbnd_test_scenarios.spark.spark_tasks import WordCountPySparkTask, WordCountTask


EXAMPLE_JVM_DEFAULTS = {
    SparkConfig.jars: None,
    SparkConfig.main_jar: __file__,
    SparkConfig.num_executors: 77,
}


def my_submit_run(*args):
    output_path = args[0]["spark_submit_task"]["parameters"][-1]
    with open(output_path, "w") as file:
        file.write("test was ok")
    return 66 + len(args)


class TestDatabricks(object):
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
            override=EXAMPLE_JVM_DEFAULTS,
        )
        t.dbnd_run()

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
            override=EXAMPLE_JVM_DEFAULTS,
        ).dbnd_run()
