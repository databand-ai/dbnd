# © Copyright Databand.ai, an IBM Company 2022

import pytest

from databand.parameters import TaskParameter
from dbnd import PipelineTask, dbnd_run_cmd, output
from dbnd._core.errors import DatabandError
from dbnd_test_scenarios.test_common.task.factories import TTask


class MNTask1(TTask):
    task_namespace = "mynamespace"


class MNTask2(TTask):
    task_namespace = "mynamespace"
    t_param = TaskParameter()


class OtherTask(TTask):
    task_namespace = "other_namespace"


class TestTaskParameter(object):
    def test_task_param(self):
        assert MNTask2(t_param=MNTask2).t_param == MNTask2
        assert MNTask2(t_param=OtherTask).t_param == OtherTask

    def test_create(self):
        assert MNTask2(t_param="mynamespace.MNTask1")

    def test_failed_to_find_task(self):
        # But is should be able to parse command line arguments
        with pytest.raises(DatabandError):
            dbnd_run_cmd("mynamespace.MNTask2 -r t_param=blah")
        with pytest.raises(DatabandError):
            dbnd_run_cmd("mynamespace.MNTask2 -r t_param=Taskk")

    def test_simple_cmd_line(self):
        result = dbnd_run_cmd("mynamespace.MNTask2 -r t_param=mynamespace.MNTask1")
        assert isinstance(result.task.t_param, MNTask1)

    def test_serialize(self):
        class DepTask(PipelineTask):
            task_param = TaskParameter()

            some_output = output

            def band(self):
                self.some_output = self.task_param()

        class MainTask(PipelineTask):
            some_other_output = output

            def band(self):
                self.some_other_output = DepTask(task_param=TTask)

        # OtherTask is serialized because it is used as an argument for DepTask.
        assert dbnd_run_cmd(["MainTask"])
