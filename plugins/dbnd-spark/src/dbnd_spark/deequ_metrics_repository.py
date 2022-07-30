# © Copyright Databand.ai, an IBM Company 2022

import logging

from py4j.protocol import Py4JError, Py4JJavaError
from pydeequ.repository import MetricsRepository


logger = logging.getLogger(__name__)


class DbndMetricsRepository(MetricsRepository):
    """
    Deequ metrics repository implementation which submits all metrics to the Databand.
    It will proxy all calls to the Java implementation.
    This repository will work only if dbnd-api-deequ jar was added to the classpath.
    Otherwise, Deequ metrics won't be reported.

    Former implementation was used Java converter to convert Deequ metrics to dict and submit it
    via python log_metric API.
    However, that implementation doesn't work on Dataproc (it requires back channel from java to python),
    so we've switched to direct reporting from Java code.
    Task context are managed via set_external_context method which instructs JVM where to send metrics.
    """

    def __init__(self, spark_session):
        self._spark_session = spark_session
        self._jvm = spark_session._jvm
        self._jspark_session = spark_session._jsparkSession
        try:
            self.deequDBNDmetRep = (
                spark_session._jvm.ai.databand.deequ.DbndMetricsRepository
            )
            self.repository = self.deequDBNDmetRep()
        except (Py4JJavaError, Py4JError, TypeError):
            logger.error(
                "Unable to load Databand Java SDK. Deequ metrics won't be reported to Databand. "
                "Perhaps you forgot to add dbnd-api-deequ to spark classpath? "
                "Built-in Deequ in-memory metrics repository will be used instead."
            )
            self.deequIMmetRep = (
                spark_session._jvm.com.amazon.deequ.repository.memory.InMemoryMetricsRepository
            )
            self.repository = self.deequIMmetRep()
