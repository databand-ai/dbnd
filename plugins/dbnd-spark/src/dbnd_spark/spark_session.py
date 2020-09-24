import logging
import sys

from dbnd._core.utils.seven import import_errors


logger = logging.getLogger(__name__)


def get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def has_spark_session():
    if not has_pyspark_imported():
        logger.debug("Spark not found in modules")
        return False

    try:
        from pyspark.sql import SparkSession

        logger.debug(
            "Spark session SparkSession._instantiatedSession %s",
            SparkSession._instantiatedSession,
        )
        return SparkSession._instantiatedSession is not None
    except import_errors:
        return False


def has_pyspark_imported():
    if "pyspark" not in sys.modules:
        logger.debug("Spark not found in modules")
        return False
    return True
