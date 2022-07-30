# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd import config, relative_path
from dbnd.testing.helpers_pytest import assert_run_task


def _data_for_spark_path(*path):
    from dbnd_examples.orchestration.dbnd_spark import read_from_multiple_sources

    return relative_path(
        read_from_multiple_sources.__file__, "data_for_spark_examples", *path
    )


@pytest.mark.spark
class TestPysparkTasks(object):
    def test_spark_inline_same_context(self):
        from pyspark.sql import SparkSession

        from dbnd_examples.orchestration.dbnd_spark.word_count import word_count_inline
        from dbnd_spark.local.local_spark_config import SparkLocalEngineConfig

        with SparkSession.builder.getOrCreate() as sc:
            with config({SparkLocalEngineConfig.enable_spark_context_inplace: True}):
                task_instance = word_count_inline.t(text=__file__)
                assert_run_task(task_instance)

    def test_read_from_multiple_sources(self):
        from pyspark.sql import SparkSession

        from dbnd_examples.orchestration.dbnd_spark.read_from_multiple_sources import (
            data_source_complicated_pipeline,
        )
        from dbnd_spark.local.local_spark_config import SparkLocalEngineConfig

        with SparkSession.builder.getOrCreate() as sc:
            with config({SparkLocalEngineConfig.enable_spark_context_inplace: True}):
                task_instance = data_source_complicated_pipeline.t(
                    root_path=_data_for_spark_path("read_from_multiple_sources"),
                    extra_data=_data_for_spark_path(
                        "read_from_multiple_sources1", "configID=1", "1.tsv"
                    ),
                )
                assert_run_task(task_instance)
