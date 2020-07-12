import sys

from dbnd._core.utils.seven import import_errors


def get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def has_spark_session():
    if "pyspark" not in sys.modules:
        return False

    try:
        from pyspark.sql import SparkSession

        return SparkSession._instantiatedSession is not None
    except import_errors:
        return False
