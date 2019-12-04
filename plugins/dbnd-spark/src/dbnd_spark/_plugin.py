import dbnd

from dbnd import register_config_cls
from dbnd_spark.livy.livy_spark_config import LivySparkConfig


@dbnd.hookimpl
def dbnd_on_pre_init_context(ctx):
    from dbnd_spark.local.local_spark_config import SparkLocalConfig

    register_config_cls(SparkLocalConfig)
    register_config_cls(LivySparkConfig)
