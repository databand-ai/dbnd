import time

import pytest

from airflow import settings

from dbnd import PipelineTask, parameter
from dbnd._core.errors import DatabandConfigError
from dbnd._core.errors.base import DatabandRunError
from dbnd._core.inline import run_cmd_locally, run_task
from dbnd.tasks.basics import SimplestTask
from dbnd.testing import assert_run_task
from dbnd.testing.helpers_pytest import skip_on_windows


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
        run_task(target)
        assert target._complete()

    # @with_context(conf={'executor': {'local': 'true'},
    #                     'databand': {'module': ParallelTasksPipeline.__module__}})
    @skip_on_windows
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
            with pytest.raises(DatabandRunError):  # not supported on sqlite
                run_cmd_locally(cmd)
        else:
            run_cmd_locally(cmd)

    def test_parallel_dag_locally(self):
        task = ParallelTasksPipeline(override={SleepyTask.sleep_time: 0})
        assert_run_task(task)
        # target = ParallelTasksPipeline(num_of_tasks=2)
        # run_task(target)
