# © Copyright Databand.ai, an IBM Company 2022

import logging

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
