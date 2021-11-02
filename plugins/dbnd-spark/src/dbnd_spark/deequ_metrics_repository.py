import logging

from dbnd import log_metrics
from py4j.protocol import Py4JJavaError
from pydeequ.repository import MetricsRepository


logger = logging.getLogger(__name__)


class DeequMetricsRepository(object):
    def save(self, result_key, analyzer_context):
        pass

    def loadByKey(self, result_key):
        pass

    def load(self):
        pass

    class Java:
        implements = ["com.amazon.deequ.repository.MetricsRepository"]


class DbndDeequMetricsRepository(DeequMetricsRepository):
    def __init__(self, spark_session):
        self.jvm = spark_session._jvm
        try:
            # check if dbnd jar was loaded
            self.jvm.ai.databand.deequ.NoopMetricsRepository()
            self.java_libs_loaded = True
        except Py4JJavaError:
            logger.error(
                "Unable to load Databand Java SDK. Perhaps you forgot to add dbnd to spark classpath?"
            )
            self.java_libs_loaded = False

    def save(self, result_key, analyzer_context):
        if not self.java_libs_loaded:
            logger.warn(
                "Databand Java SDK was not loaded. Deequ metrics won't be logged"
            )
            return
        df_name = (
            self.jvm.scala.collection.JavaConverters.mapAsJavaMapConverter(
                result_key.tags()
            )
            .asJava()
            .getOrDefault("name", "data")
        )
        converter = self.jvm.ai.databand.deequ.DeequToDbnd(df_name, analyzer_context)
        metrics = dict(converter.metrics())
        log_metrics(metrics)

    def loadByKey(self, result_key):
        return self.jvm.scala.Option.empty()


class DbndMetricsRepository(MetricsRepository):
    def __init__(self, spark_session):
        self.repository = DbndDeequMetricsRepository(spark_session)
