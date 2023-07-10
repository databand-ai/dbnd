# © Copyright Databand.ai, an IBM Company 2022

import pyspark.sql as spark

from dbnd_examples_orchestration.dbnd_spark import spark_folder
from dbnd_examples_orchestration.orchestration.dbnd_spark.scripts import spark_script

from databand import output, parameter, pipeline
from databand.tasks import PipelineTask
from dbnd import log_dataframe, log_metric
from dbnd_spark.spark import PySparkInlineTask, PySparkTask, SparkTask, spark_task
from dbnd_spark.spark_config import SparkConfig
from targets import Target


COUNT_WITH_HTML_MAIN_CLASS = "ai.databand.examples.WordCountWithHtml"
WORD_COUNT_MAIN_CLASS = "ai.databand.examples.WordCount"


def _mvn_target_file(*path):
    return spark_folder("spark_jvm/target", *path)


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


@spark_task(result=output[spark.DataFrame])
def word_count_inline(text=parameter.csv[spark.DataFrame], counters=output.txt.data):
    # type:  (spark.DataFrame, Target) -> spark.DataFrame
    from operator import add

    from dbnd_spark.spark import get_spark_session

    lines = text.rdd.map(lambda r: r[0])
    counts = (
        lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
    )
    counts.saveAsTextFile(str(counters))
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    counts_df = get_spark_session().createDataFrame(counts)
    log_dataframe("counts_df", counts_df)
    log_metric("test", 1)

    return counts_df


class WordCountSparkInline(PySparkInlineTask):
    text = parameter.csv[spark.DataFrame]
    counters = output.txt.data
    counters_auto_save = output[spark.DataFrame]

    def run(self):
        from operator import add

        from dbnd_spark.spark import get_spark_session

        lines = self.text.rdd.map(lambda r: r[0])
        counts = (
            lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
        )
        counts.saveAsTextFile(str(self.counters))
        output = counts.collect()
        for (word, count) in output:
            print("%s: %i" % (word, count))

        self.counters_auto_save = get_spark_session().createDataFrame(counts)
