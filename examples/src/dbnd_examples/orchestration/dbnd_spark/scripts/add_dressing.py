import datetime
import logging
import sys


logger = logging.getLogger(__name__)


def run_spark(args):
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, StructField, StructType

    timestamp = str(datetime.datetime.now()).replace(" ", "_")

    spark = SparkSession.builder.appName("Task_%s" % timestamp).getOrCreate()
    sc = spark.sparkContext

    veg = sc.textFile(args[1]).collect()

    mix = ["%s->%s" % (args[2], v) for v in veg]
    result = "".join(mix)

    logger.info("Dressing result %s", result)

    fields = [StructField("salad_with_dressing", StringType(), True)]
    schema = StructType(fields)

    df = spark.createDataFrame([(veg,)], schema)
    df.coalesce(1).write.csv(args[3] + timestamp)
    sc.stop()


if __name__ == "__main__":
    run_spark(sys.argv)
