import logging

from datetime import date
from typing import Dict

from dbnd import ParameterScope, PipelineTask, data, output, parameter, task
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase


logger = logging.getLogger(__name__)


class TestParameterScope(TargetTestBase):
    def test_simple_pipeline_children_scope(self):
        expected_task_target_date = date(2020, 1, 1)
        expected_tstr = "teststr"
        expected_tdata = __file__

        @task
        def simple_task(tdata, tstr):
            pass

        class TPipeline(PipelineTask):
            tdata = data(scope=ParameterScope.children)
            tstr = parameter(scope=ParameterScope.children)[str]

            some_a = output

            def band(self):
                self.some_a = simple_task()

        t_pipeline = TPipeline(
            tdata=expected_tdata,
            tstr=expected_tstr,
            task_target_date=expected_task_target_date,
        )
        t_task = t_pipeline.some_a.task

        assert t_task.task_target_date == expected_task_target_date
        assert str(t_task.tdata) == expected_tdata
        assert t_task.tstr == expected_tstr

    def test_dict_passing(self):
        @task
        def simple_task(tdict, tdict_typed):
            # type: (Dict, Dict[str,str]) -> str
            return "OK"

        class TPipeline(PipelineTask):
            tdict = parameter(scope=ParameterScope.children)[Dict]
            tdict_typed = parameter(scope=ParameterScope.children)[Dict[str, str]]

            some_a = output

            def band(self):
                self.some_a = simple_task()

        expected_tdict = {"a": 1}
        expected_tdict_typed = {"a": "1"}

        t_pipeline = TPipeline(tdict=expected_tdict, tdict_typed=expected_tdict_typed)
        t_task = t_pipeline.some_a.task

        assert t_task.tdict == expected_tdict
        assert t_task.tdict_typed == expected_tdict_typed
