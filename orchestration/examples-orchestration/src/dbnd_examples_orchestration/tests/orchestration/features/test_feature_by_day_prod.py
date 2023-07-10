# © Copyright Databand.ai, an IBM Company 2022

import logging

import pytest

from dbnd_examples_orchestration.orchestration.features.partitioned_by_day import (
    ByDayExamplePipeline,
    FetchData,
    FetchIds,
    ProductionIdsAndData,
)

from dbnd import dbnd_config, get_databand_context
from dbnd_run.testing.helpers import assert_run_task


logger = logging.getLogger(__name__)


class TestPartitionsByDate(object):
    def test_prod_immutable_output_example(self):
        with dbnd_config(
            {FetchIds.task_enabled_in_prod: True, FetchData.task_enabled_in_prod: True}
        ):
            task = ProductionIdsAndData(
                task_env=get_databand_context().run_settings.env.clone(production=True)
            )
            assert_run_task(task)

    def test__by_day_simple_local(self):
        with dbnd_config(
            {
                ProductionIdsAndData.task_env: "local",
                FetchIds.task_enabled_in_prod: True,
                FetchData.task_enabled_in_prod: True,
            }
        ):
            assert_run_task(ByDayExamplePipeline(period="2d"))

    @pytest.mark.aws
    @pytest.mark.skip("aws permissions are wrong")
    def test__by_day_simple_aws(self):
        with dbnd_config(
            {FetchIds.task_enabled_in_prod: True, FetchData.task_enabled_in_prod: True}
        ):
            assert_run_task(ByDayExamplePipeline(period="2d"))
