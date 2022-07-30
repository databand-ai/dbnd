# © Copyright Databand.ai, an IBM Company 2022

import datetime

from datetime import timedelta
from os import path
from typing import List

from dbnd import PipelineTask, data, output, parameter
from dbnd.tasks import Config, DataSourceTask
from dbnd.tasks.basics import SimplestTask
from dbnd.utils import data_combine, period_dates
from targets import target
from targets.target_config import folder


class TDataSource(DataSourceTask):
    partitioned_data_path = path.join(
        path.dirname(__file__), "data", "partitioned_data"
    )
    root_location = data(default=partitioned_data_path)
    task_target_date = parameter()[datetime.date]

    logs = output

    def band(self):
        self.logs = target(
            self.root_location,
            "%s/" % self.task_target_date.strftime("%Y-%m-%d"),
            config=folder.without_flag(),
        )


class AT_1(SimplestTask):
    def run(self):
        self.log_metric("some_metric", 1)
        self.log_metric("some_metric1", 2.0)
        self.log_metric("m_string", "my_metric")
        self.log_metric("m_tuple", (1, 2, "complicated"))
        super(AT_1, self).run()


class TComplicatedTask(SimplestTask):
    specific_input = data.target
    task_input = data.target
    some_param = parameter.value(1)

    def run(self):
        self.log_metric("some_metric", 1)
        self.log_metric("some_metric1", 2.0)
        self.log_metric("m_string", "my_metric")
        self.log_metric("m_tuple", (1, 2, "complicated"))
        super(TComplicatedTask, self).run()


class TNestedPipeline1(PipelineTask):
    parameter_with_huge_value = parameter.value(default="some_v2_" * 20)
    some_output = output

    def band(self):
        self.some_output = AT_1(simplest_param="2").simplest_output


class TNestedPipeline2(PipelineTask):
    some_output = output
    parameter_with_huge_value = parameter(default="some_v1_" * 20)

    def band(self):
        self.some_output = AT_1(simplest_param="2").simplest_output


class TSuperNestedPipeline(PipelineTask):
    some_output = output
    parameter_with_huge_value = parameter(default="some_v1_" * 20)

    list_parameter = parameter[List]

    def band(self):
        self.some_output = TNestedPipeline2(parameter_with_huge_value="2").some_output


class SomeConfig(Config):
    some_param = parameter(default=1)


class TComplicatedPipeline(PipelineTask):
    period = parameter(default=timedelta(days=2))

    list_param = parameter(default=["a", "b"])[List]
    task_target_date = "2018-09-03"  # data exists only for this date

    list_tasks_output = output
    list_output = output
    combined_output = output

    nested = output
    nested2 = output

    parameter_with_huge_value = parameter(default="some_value_" * 20)

    def band(self):
        all_tasks = []
        my_config = SomeConfig()
        for i, d in enumerate(period_dates(self.task_target_date, self.period)):
            source = TDataSource(task_target_date=d)
            complicated = TComplicatedTask(
                specific_input=source,
                some_param=my_config.some_param,
                task_input=source.logs,
                # empty_input=None,
                task_target_date=d,
            )
            all_tasks.append(complicated)

        self.combined_output = data_combine([t.simplest_output for t in all_tasks])
        self.list_output = [t.simplest_output for t in all_tasks]
        self.list_tasks_output = [t for t in all_tasks]

        self.nested = [
            TNestedPipeline1(task_name="MyNewPipe").some_output,
            TNestedPipeline1(task_name="custom_task_name").some_output,
        ]

        self.nested2 = TNestedPipeline2().some_output
        self.nested3 = TSuperNestedPipeline(list_parameter=self.list_param).some_output
