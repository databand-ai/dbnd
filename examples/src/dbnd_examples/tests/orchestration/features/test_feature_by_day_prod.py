import logging

import pytest

from dbnd import dbnd_config, get_databand_context
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.orchestration.features.partitioned_by_day import (
    ByDayExamplePipeline,
    FetchData,
    FetchIds,
    ProductionIdsAndData,
)


logger = logging.getLogger(__name__)


class TestPartitionsByDate(object):
    def test_prod_immutable_output_example(self):
        with dbnd_config(
            {FetchIds.task_enabled_in_prod: True, FetchData.task_enabled_in_prod: True}
        ):
            task = ProductionIdsAndData(
                task_env=get_databand_context().env.clone(production=True)
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
