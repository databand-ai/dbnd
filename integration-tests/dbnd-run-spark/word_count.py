from databand import output, parameter, pipeline
from databand.tasks import PipelineTask
from dbnd_examples.dbnd_spark import spark_folder
from dbnd_examples.orchestration.dbnd_spark.scripts import spark_script
from dbnd_spark import PySparkTask, SparkTask
from dbnd_spark.spark_config import SparkConfig


COUNT_WITH_HTML_MAIN_CLASS = "ai.databand.examples.WordCountWithHtml"
WORD_COUNT_MAIN_CLASS = "ai.databand.examples.WordCount"


def _mvn_target_file(*path):
    return spark_folder("spark-jvm/target", *path)


class WordCountTask(SparkTask):
    text = parameter.data
    counters = output

    main_class = WORD_COUNT_MAIN_CLASS

    defaults = {SparkConfig.driver_memory: "2G"}

    def application_args(self):
        return [self.text, self.counters]


class WordCountPySparkTask(PySparkTask):
    text = parameter.data
    counters = output

    python_script = spark_script("word_count.py")

    def application_args(self):
        return [self.text, self.counters]


class WordCountPipeline(PipelineTask):
    text = parameter.data

    with_spark = output
    with_pyspark = output

    def band(self):
        self.with_spark = WordCountTask(text=self.text)
        self.with_pyspark = WordCountPySparkTask(text=self.text)


@pipeline
def word_count_new_cluster():
    wc = WordCountTask()

    from dbnd_gcp.dataproc.dataproc import DataProcCtrl

    create = DataProcCtrl(wc).create_engine()
    wc.set_upstream(create)
