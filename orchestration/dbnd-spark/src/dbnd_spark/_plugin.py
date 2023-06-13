# © Copyright Databand.ai, an IBM Company 2022

import logging

import dbnd

from dbnd import register_config_cls
from dbnd_spark.livy.livy_spark_config import LivySparkConfig


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd_spark.local.local_spark_config import SparkLocalEngineConfig
    from dbnd_spark.spark_bootstrap import workaround_spark_namedtuple_serialization

    register_config_cls(SparkLocalEngineConfig)
    register_config_cls(LivySparkConfig)

    workaround_spark_namedtuple_serialization()
