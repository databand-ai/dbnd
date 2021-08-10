import pyspark.sql as spark

from dbnd import output, parameter
from dbnd.tasks import spark_task
from targets.types import PathStr


@spark_task(result=output[spark.DataFrame])
def word_count_inline(
    text=parameter.data.txt[spark.DataFrame], counters=output.txt.data
):
    # type:  (spark.DataFrame, PathStr) -> spark.DataFrame
    from operator import add
    from dbnd_spark import get_spark_session

    lines = text.rdd.map(lambda r: r[0])
    counts = (
        lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
    )
    counts.saveAsTextFile(str(counters))
    df = get_spark_session().createDataFrame(counts)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    return df
