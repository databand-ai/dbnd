# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import timedelta

from dbnd_examples_orchestration.data import data_repo
from dbnd_examples_orchestration.orchestration.features.partitioned_calculations_pipeline import (
    ExamplePartitionedCalculations,
)
from dbnd_examples_orchestration.orchestration.features.partitioned_data_pipeline import (
    ExamplePartitionedDataPipeline,
)
from dbnd_examples_orchestration.orchestration.features.partitioned_inputs import (
    DeviceLogsPipeline,
    ExamplePartitionedPipeline,
)

from dbnd_run.testing.helpers import assert_run_task


logger = logging.getLogger(__name__)


class TestFeaturePartitions(object):
    def test_partitions(self):
        target = ExamplePartitionedPipeline(
            task_target_date=data_repo.partitioned_data_target_date
        )
        assert_run_task(target)

    def test_pipeline_with_logs(self):
        task = assert_run_task(
            ExamplePartitionedPipeline(
                task_target_date=data_repo.partitioned_data_target_date
            )
        )
        logger.error(task.features.read())

    def test_paritioned_input(self):
        assert_run_task(
            DeviceLogsPipeline(
                period=timedelta(days=1),
                task_target_date=data_repo.partitioned_data_target_date,
            )
        )

    def test_partitioned_data_task(self):
        task = ExamplePartitionedCalculations()
        assert_run_task(task)

    def test_partitioned_batch_task(self):
        task = ExamplePartitionedDataPipeline()
        assert_run_task(task)
