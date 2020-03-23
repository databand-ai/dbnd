import dbnd

from dbnd import register_config_cls
from dbnd_spark.livy.livy_spark_config import LivySparkConfig


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd_spark.local.local_spark_config import SparkLocalEngineConfig
    from dbnd_spark.spark_bootstrap import dbnd_spark_bootstrap

    register_config_cls(SparkLocalEngineConfig)
    register_config_cls(LivySparkConfig)

    dbnd_spark_bootstrap()
