import pyspark.sql as spark

from databand import output, parameter
from dbnd import pipeline, task
from dbnd_spark.spark import spark_output, spark_task
from targets.target_config import FileFormat


@pipeline(result=spark_output.csv[spark.DataFrame])
def dataframes_io_pandas_spark(text=parameter.txt[spark.DataFrame]):
    # SPARK: INPUT = txt file, OUTPUT= folder csv
    output1 = word_count_inline(text)
    # PANDAS: INPUT = csv folder OUTPUT = csv file
    output2 = python_after_spark(output1)
    # SPARK: INPUT = csv folder OUTPUT = csv folder
    output2 = word_count_inline_folder(output1)
    return output2


@spark_task(result=spark_output.csv[spark.DataFrame])
def word_count_inline(text=parameter.txt[spark.DataFrame]):
    # type:  (spark.DataFrame) -> spark.DataFrame
    from operator import add
    from dbnd_spark.spark import get_spark_session

    lines = text.rdd.map(lambda r: r[0])
    counts = (
        lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
    )
    # counts.saveAsTextFile(str(counters))
    df = get_spark_session().createDataFrame(counts)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    return df


@spark_task(result=spark_output.csv[spark.DataFrame])
def word_count_inline_folder(text=parameter.folder.csv[spark.DataFrame]):
    # type:  (spark.DataFrame) -> spark.DataFrame
    from operator import add
    from dbnd_spark.spark import get_spark_session

    lines = text.rdd.filter(lambda r: r[0] is not None).map(lambda r: r[0])
    counts = (
        lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
    )
    # counts.saveAsTextFile(str(counters))
    df = get_spark_session().createDataFrame(counts)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    return df


@task(result=output.csv[spark.DataFrame])
def python_after_spark(counters=parameter.folder.csv[spark.DataFrame]):
    for i in counters:
        print(i)
    return counters
