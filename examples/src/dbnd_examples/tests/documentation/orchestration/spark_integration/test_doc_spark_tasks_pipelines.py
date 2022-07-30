# © Copyright Databand.ai, an IBM Company 2022

"""import pyspark.sql as spark

from dbnd import PipelineTask, output, parameter
from dbnd_examples.dbnd_spark import spark_folder
from dbnd_spark import SparkConfig
from dbnd_spark.spark import PySparkTask, SparkTask, spark_task


#### DOC START


@spark_task(result=output[spark.DataFrame])
def word_count_inline(text=parameter.txt[spark.DataFrame]):
    from operator import add
    from dbnd_spark.spark import get_spark_session

    lines = text.rdd.map(lambda r: r[0])
    counts = (
        lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
    )

    return get_spark_session().createDataFrame(counts.collect())


class WordCountPySparkTask(PySparkTask):
    text = parameter.data
    counters = output

    python_script = spark_folder("pyspark/word_count.py")

    def application_args(self):
        return [self.text, self.counters]


class WordCountTask(SparkTask):
    text = parameter.data
    counters = output

    main_class = "ai.databand.examples.WordCount"
    defaults = {
        SparkConfig.main_jar: "${DBND_HOME}/databand_examples/tool_spark/spark_jvm/target/ai.databand.examples-1.0-SNAPSHOT.jar"
    }

    def application_args(self):
        return [self.text, self.counters]


class AllWordCounts(PipelineTask):
    text = parameter.data

    counters_python = output.csv.data
    counters_java = output.csv.data

    def band(self):
        t_text = CloneFile(text=self.text)

        self.counters_python = WordCountPySparkTask(text=t_text.clone)
        self.counters_java = WordCountTask(t_text.clone)


#### DOC END"""
