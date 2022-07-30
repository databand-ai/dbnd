# © Copyright Databand.ai, an IBM Company 2022

import datetime

from datetime import timedelta

import dbnd

from dbnd import data, output, parameter
from dbnd.tasks import DataSourceTask, PythonTask
from dbnd.utils import data_combine, period_dates
from dbnd_examples.data import data_repo
from targets import target
from targets.target_config import folder


class RawDeviceLog(DataSourceTask):
    """raw device logs for partitioned pipeline"""

    task_target_date = parameter[datetime.date]
    root_location = data.default(data_repo.partitioned_data)

    logs = output

    def band(self):
        self.logs = target(
            self.root_location,
            "%s/" % self.task_target_date.strftime("%Y-%m-%d"),
            config=folder.without_flag(),
        )


class DeviceLogProjection(PythonTask):
    """normalize device logs for partitioned pipeline"""

    raw_logs = data.target

    projected_logs = output.target

    def run(self):
        self.projected_logs.write("\n".join(self.raw_logs.readlines()))


class DeviceLogsPipeline(dbnd.PipelineTask):
    """project device logs for partitioned pipeline"""

    period = parameter(default=timedelta(days=2))[timedelta]
    projected = output

    def band(self):
        projected_logs = []
        for i, d in enumerate(period_dates(self.task_target_date, self.period)):
            raw_logs = RawDeviceLog(task_target_date=d).logs
            projected = DeviceLogProjection(raw_logs=raw_logs, task_target_date=d)
            projected_logs.append(projected.projected_logs)
        self.projected = data_combine(projected_logs)


class Features(PythonTask):
    """create features for partitioned pipeline"""

    projected_logs = data.target
    features = output.target

    def run(self):
        data = self.projected_logs.readlines()
        self.features.write("".join(data))


class ExamplePartitionedPipeline(dbnd.PipelineTask):
    """Entry point for partitioned pipeline"""

    features = output.target

    def band(self):
        projected = DeviceLogsPipeline().projected
        self.features = Features(projected_logs=projected).features
