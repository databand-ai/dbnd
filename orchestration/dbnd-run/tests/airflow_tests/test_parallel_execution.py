# © Copyright Databand.ai, an IBM Company 2022

import time

import pytest

from airflow import settings

from dbnd import PipelineTask, dbnd_run_cmd, parameter
from dbnd._core.errors import DatabandConfigError
from dbnd_run.tasks.basics import SimplestTask
from dbnd_run.testing.helpers import assert_run_task


class SleepyTask(SimplestTask):
    sleep_time = parameter.value(0.1, significant=False)

    def run(self):
        if self.sleep_time:
            time.sleep(self.sleep_time)
        super(SleepyTask, self).run()


class ParallelTasksPipeline(PipelineTask):
    num_of_tasks = parameter.value(3)

    def band(self):
        tasks = []
        for i in range(self.num_of_tasks):
            tasks.append(SleepyTask(simplest_param=str(i)))
        return tasks


class TestTasksParallelExample(object):
    def test_parallel_simple_executor(self):
        target = ParallelTasksPipeline(num_of_tasks=2)
        target.dbnd_run()
        assert target._complete()

    def test_parallel_local_executor(self):
        cmd = [
            "-m",
            ParallelTasksPipeline.__module__,
            ParallelTasksPipeline.get_task_family(),
            "--parallel",
            "-r",
            "num_of_tasks=2",
        ]

        if "sqlite" in settings.SQL_ALCHEMY_CONN:
            with pytest.raises(DatabandConfigError):  # not supported on sqlite
                dbnd_run_cmd(cmd)
        else:
            dbnd_run_cmd(cmd)

    def test_parallel_dag_locally(self):
        task = ParallelTasksPipeline(override={SleepyTask.sleep_time: 0})
        assert_run_task(task)
        # target = ParallelTasksPipeline(num_of_tasks=2)
        # target.dbnd_run()
