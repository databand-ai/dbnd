def get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def has_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession._instantiatedSession is not None
