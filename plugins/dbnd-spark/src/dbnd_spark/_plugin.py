import logging

import dbnd

from dbnd import dbnd_config, register_config_cls
from dbnd._core.configuration.config_readers import get_environ_config_from_dict
from dbnd._core.configuration.environ_config import (
    _debug_init_print,
    spark_tracking_enabled,
)
from dbnd._core.settings.histogram import HistogramConfig
from dbnd_spark.livy.livy_spark_config import LivySparkConfig
from dbnd_spark.spark_session import has_pyspark_imported


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd_spark.local.local_spark_config import SparkLocalEngineConfig
    from dbnd_spark.spark_bootstrap import dbnd_spark_bootstrap

    register_config_cls(SparkLocalEngineConfig)
    register_config_cls(LivySparkConfig)

    dbnd_spark_bootstrap()

    if has_pyspark_imported() and spark_tracking_enabled():
        config_store = read_spark_environ_config()
        dbnd_config.set_values(config_store, "system")
    else:
        _debug_init_print(
            "spark conf is not loaded since pyspark is not imported or DBND__ENABLE__SPARK_CONTEXT_ENV is not set"
        )


def read_spark_environ_config():
    logger.debug("running read_spark_environ_config")

    from pyspark import SparkContext

    _debug_init_print("creating spark context to read spark conf")
    spark_conf = SparkContext.getOrCreate().getConf()
    spark_conf = dict(spark_conf.getAll())
    spark_conf = {key.lstrip("spark.env."): value for key, value in spark_conf.items()}

    return get_environ_config_from_dict(spark_conf, "environ")
