import logging
import sys

from dbnd._core.utils.seven import import_errors
from dbnd_spark.spark_listener import DbndSparkListener


logger = logging.getLogger(__name__)


def get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def shutdown_callback_server():
    session = get_spark_session()
    sc = session.sparkContext
    logger.info("Removing Databand Spark Listener and shutting down callback server")
    # Our spark listener should be removed before closing out callback server.
    # Otherwise it will print arcane exceptions to stdout.
    listener = find_dbnd_listener(sc)
    if listener:
        sc._jsc.sc().removeSparkListener(listener)

    sc._gateway.shutdown_callback_server()


def inject_spark_listener(session):
    sc = session.sparkContext
    if find_dbnd_listener(sc):
        logger.info("Databand Spark Listener was already injected")
        pass
    else:
        logger.info("Injecting Databand Spark Listener")
        sc._gateway.start_callback_server()
        sc._jsc.sc().addSparkListener(DbndSparkListener())
        # suppress py4j callback server log messages
        logging.getLogger("py4j").setLevel(logging.ERROR)


def find_dbnd_listener(sc):
    listeners = sc._jsc.sc().listenerBus().listeners()
    for i in range(0, listeners.size()):
        next_listener = listeners.get(i)
        if next_listener.toString() == DbndSparkListener.__name__:
            return next_listener
    return None


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
