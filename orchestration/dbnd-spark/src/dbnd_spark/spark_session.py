# © Copyright Databand.ai, an IBM Company 2022

import logging


logger = logging.getLogger(__name__)


def get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()
