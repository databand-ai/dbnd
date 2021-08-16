#### DOC START
from dbnd import output, parameter
from dbnd_spark import SparkTask


class WordCountTask(SparkTask):
    text = parameter.data
    counters = output

    spark_conf_extension = {"spark.executor.memory": "1g"}


#### DOC END
