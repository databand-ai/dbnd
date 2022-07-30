# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import timedelta
from typing import List

from pandas import DataFrame

import dbnd

from dbnd import ParameterScope, data, output, parameter, task
from dbnd.tasks import PythonTask
from dbnd.utils import data_combine, period_dates
from targets.types import DataList


logger = logging.getLogger(__name__)

one_day = timedelta(days=1)

TUTORIAL = """

dbnd run FetchedData --task-env prod --period 10d

dbnd run ByDayExamplePipeline  --period "10d"  --FetchedData--disable-run

dbnd run ByDayExamplePipeline  --period "10d"  --FetchedData--disable-run


"""


def cb_data_dump_path(task, target_date, name):
    pass


class FetchIds(PythonTask):
    """Fetch ids by period"""

    period = parameter[timedelta]
    ids = output.prod_immutable[List[str]]
    labels = output.prod_immutable[List[str]]

    task_enabled_in_prod = False

    def run(self):
        values = ["a", "b", "c"]
        self.ids = ["%s-%s\n" % (s, self.task_target_date) for s in values]
        self.labels = ["%s-%s YES\n" % (s, self.task_target_date) for s in values]


class FetchData(PythonTask):
    """Fetch data of ids"""

    ids = data[List[str]]
    data = output.prod_immutable[List[str]]

    task_enabled_in_prod = False

    def run(self):
        values = ["a", "b", "c"]
        self.data = ["data-%s-%s" % (s, self.task_target_date) for s in values]


class IdsAndData(dbnd.PipelineTask):
    """Enrich Ids with data"""

    period = parameter.value(one_day)

    ids = output
    data = output

    def band(self):
        all_ids, all_data = {}, {}
        for i, d in enumerate(period_dates(self.task_target_date, self.period)):
            # if self.task_env == TaskEnv.prod and not self.run_on_prod:
            #     ids = cb_data_dump_path(task_target_date=d, name="ids")
            #     data = cb_data_dump_path(task_target_date=d, name="data")
            # else:
            ids = FetchIds(task_target_date=d, period=one_day).ids
            data = FetchData(task_target_date=d, ids=ids).data

            d_key = d.strftime("%Y-%m-%d")
            all_ids[d_key] = ids
            all_data[d_key] = data
        self.ids = data_combine(all_ids.values(), sort=True)
        self.data = data_combine(all_data.values(), sort=True)


class ProductionIdsAndData(IdsAndData):
    """Enrich Ids with data (Production)"""

    task_env = "aws_prod"


class Features(PythonTask):
    ids = data.target
    data = data.target

    features = output

    def run(self):
        features = []

        for ids, data in zip(self.ids.list_partitions(), self.data.list_partitions()):
            logger.info("Working on %s, %s", ids, data)
            # we put features one by one with ids
            partition = [
                "%s -> %s" % (k, v)
                for k, v in zip(
                    ids.load(value_type=List[str]), data.load(value_type=List)
                )
            ]

            features.extend(partition)
        self.features = features


def features_df(ids, data):
    # type: (DataFrame, DataFrame) -> DataFrame
    pass


@task
def features_simple(ids, data):
    # type: (DataList[str],DataList[str]) -> List[str]
    """Create features"""
    return ["%s -> %s\n" % (k.strip(), v.strip()) for k, v in zip(ids, data)]


class FeaturesPartitioned(PythonTask):
    """Create partitioned features"""

    ids = parameter.data
    data = parameter.data
    features = output

    def run(self):
        features = []

        for ids, data in zip(self.ids.list_partitions(), self.data.list_partitions()):
            logger.info("Working on %s, %s", ids, data)
            # we put features one by one with ids
            partition = [
                "%s -> %s\n" % (k, v)
                for k, v in zip(ids.load(List[str]), data.load(List[str]))
            ]

            features.extend(partition)
        self.features = features


class ByDayExamplePipeline(dbnd.PipelineTask):
    """Entry point of partitioned_by_day"""

    period = parameter(default=one_day, scope=ParameterScope.children)[timedelta]
    features = output

    partitioned_flow = parameter(
        default=False, description="Run features in paritioned mode"
    )[bool]

    def band(self):
        fetched = ProductionIdsAndData(period=self.period)

        if self.partitioned_flow:
            features_task = FeaturesPartitioned(ids=fetched.ids, data=fetched.data)
        else:
            features_task = features_simple(ids=fetched.ids, data=fetched.data)

        self.features = features_task
