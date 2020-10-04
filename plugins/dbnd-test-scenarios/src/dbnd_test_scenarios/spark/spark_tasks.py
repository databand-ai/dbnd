from dbnd import data, output
from dbnd.tasks import PySparkTask, SparkTask
from dbnd_spark.spark_config import SparkConfig
from dbnd_test_scenarios.scenarios_repo import (
    scenario_pyspark_path,
    test_scenario_target,
)


COUNT_WITH_HTML_MAIN_CLASS = "ai.databand.examples.WordCountWithHtml"
WORD_COUNT_MAIN_CLASS = "ai.databand.examples.WordCount"


def _mvn_target_file(*path):
    return test_scenario_target("jvm_spark_project/target", *path)


EXAMPLE_JVM_DEFAULTS = {
    SparkConfig.jars: [_mvn_target_file("lib/jsoup-1.11.3.jar")],
    SparkConfig.main_jar: _mvn_target_file("ai.databand.examples-1.0-SNAPSHOT.jar"),
}


class WordCountTask(SparkTask):
    text = data
    counters = output
    defaults = EXAMPLE_JVM_DEFAULTS

    main_class = WORD_COUNT_MAIN_CLASS

    def application_args(self):
        return [self.text, self.counters]


class WordCountPySparkTask(PySparkTask):
    text = data
    counters = output

    defaults = EXAMPLE_JVM_DEFAULTS

    python_script = scenario_pyspark_path("word_count.py")

    def application_args(self):
        return [self.text, self.counters]


class WordCountThatFails(PySparkTask):
    text = data
    counters = output

    python_script = scenario_pyspark_path("word_count_with_error.py")

    def application_args(self):
        return [self.text, self.counters]
