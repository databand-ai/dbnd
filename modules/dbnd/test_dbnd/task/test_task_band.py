import logging
import os

from typing import Any

import pytest
import six

from dbnd import PipelineTask, band, current_task, output, parameter, pipeline, task
from dbnd._core.errors import DatabandBuildError
from dbnd.tasks.basics.publish import publish_results
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.task.factories import TTask
from targets import Target, target
from targets.target_config import TargetConfig
from targets.types import Path, PathStr


logger = logging.getLogger(__name__)


class TestTaskBand(object):
    def test_deco_ret_task(self):
        @band
        def ret_dict():
            v = TTask(t_param=1)
            return v

        assert_run_task(ret_dict.t())

    def test_band_ret_dict(self):
        class TRetTask(PipelineTask):
            def band(self):
                return TTask(t_param=1)

        assert_run_task(TRetTask())

    def test_band_ret_task(self):
        class TMultipleOutputsPipeline(PipelineTask):
            t_types = parameter.value([1, 2])
            t_output = output

            def band(self):
                self.t_output = {t: TTask(t_param=t).t_output for t in self.t_types}

        task = TMultipleOutputsPipeline()
        assert_run_task(task)

    def test_wire_value(self):
        @task
        def calc_value(value=1.0):
            # type: (float)-> float
            return value + 0.1

        @task
        def use_value(value=1.0):
            # type: (float)-> float
            return value + 0.1

        @band(calculated=output.data)
        def wire_value(value=1.0):
            # type: (float)-> Any
            c_v = calc_value(value)
            current_task().calculated = use_value(c_v)
            return None

        target = wire_value.t(0.5)
        assert_run_task(target)
        assert target.calculated.read_obj()

    def test_task_band_complex_objects(self):
        @task
        class t_1(object):
            def __init__(self, extra_output=output[PathStr]):
                self.extra_output = extra_output

            def run(self):
                target(self.extra_output).mkdir_parent()
                open(self.extra_output, "w").write("")
                return ""

        @pipeline
        def t_pipe():
            v = t_1().extra_output
            return v

        t_pipe.dbnd_run()

    def test_wire_list(self):
        @task
        def calc_value(value=1.0):
            # type: (float)-> float
            return value + 0.1

        @task
        def aggregate(data):
            # type: ( Target ) -> str
            for partition in data:
                assert partition.exists()
                logger.info(partition)
            return "aggregated!"

        @pipeline
        def t_pipe():
            v = calc_value()
            v1 = calc_value(0.2)
            p = aggregate(data=[v, v1])
            return p, v

        t_pipe.dbnd_run()

    def test_task_band_override_outputs(self):
        @task
        def calc_value(value=1.0, output_path=output[PathStr]):
            # type: (float)-> float
            open(output_path, "w").write("hi")
            return value + 0.1

        @pipeline
        def t_pipe():
            output_path = current_task().ctrl.outputs.target(
                "outputs", config=TargetConfig(folder=True)
            )
            output_path.mkdir()
            v = calc_value(output_path=os.path.join(str(output_path), "f1"))
            return v

        t_pipe.dbnd_run()

    def test_task_band_override_with_publisher(self):
        @task
        def calc_value(value=1.0):
            # type: (float)-> float
            return value + 0.1

        @pipeline
        def t_pipe():
            v = calc_value()
            v1 = calc_value(0.2)
            p = publish_results(data=[v, v1])
            return p.published

        t_pipe.dbnd_run()

    def test_pipeline_no_set_outputs(self):
        class TPipelineNotSet(PipelineTask):
            some_output = output

            def band(self):
                pass

        with pytest.raises(
            DatabandBuildError, match="You have unassigned output 'some_output'"
        ):
            TPipelineNotSet()

    def test_pipeline_wrong_assignment(self):
        class TPipelineWrongAssignment(PipelineTask):
            some_output = output

            def band(self):
                self.some_output = PipelineTask

        with pytest.raises(DatabandBuildError, match="Failed to assign"):
            TPipelineWrongAssignment()

    def test_task_band_with_object(self):
        @task(p=output[str])
        def t_two_outputs(p):
            p.write("ddd")
            return ""

        @task
        def t_with_path_str(p_o):
            # type: (PathStr) -> str
            assert all(isinstance(v, six.string_types) for v in p_o.values())
            return ""

        @task
        def t_with_path_lib(p_o):
            # type: (Path) -> str
            assert all(isinstance(v, Path) for v in p_o.values())
            return ""

        @band
        def t_band():
            t = t_two_outputs()
            t_path = t_with_path_str(t)
            t_path_lib = t_with_path_lib(t)
            return t_path, t_path_lib

        t_band.dbnd_run()

    def test_task_not_enough_values(self):
        @task(p=output[str])
        def t_two_outputs(p):
            p.write("ddd")
            return ""

        @band
        def t_band():
            a, b = t_two_outputs()
            return a

        with pytest.raises(Exception):
            t_band.dbnd_run()
