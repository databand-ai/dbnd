import datetime
import logging
import os

from typing import Any, Dict, List, Tuple

import pandas as pd
import pytest
import six

from dbnd import (
    PipelineTask,
    Task,
    band,
    current_task,
    new_dbnd_context,
    output,
    parameter,
    pipeline,
    task,
)
from dbnd._core.errors import DatabandBuildError
from dbnd._core.errors.base import DatabandRunError
from dbnd.tasks.basics.publish import publish_results
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.task.factories import TTask
from targets import Target
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
            output_path = current_task().get_target(
                "outputs", config=TargetConfig(folder=True)
            )
            output_path.mkdir()
            v = calc_value(output_path=os.path.join(str(output_path), "f1"))
            return v

        t_pipe.dbnd_run()

    def test_dereference_values_from_pipeline(self):
        @task(result="training_set, test_set, validation_set")
        def prepare_data():
            # type: () -> Tuple[int, int, int]
            train_df, test_df, validation_df = 13, 17, 19
            return train_df, test_df, validation_df

        @pipeline
        def uber_wiring():
            p = prepare_data()
            assert p[0].path.endswith("training_set.pickle")
            assert p["test_set"].path.endswith("test_set.pickle")

            with pytest.raises(IndexError) as e:
                p[42]
            assert "target" in str(e.value)

            with pytest.raises(AttributeError) as e:
                p["makaron"]
            assert "target" in str(e.value)

        uber_wiring.dbnd_run()

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

    def test_circle_dependency_type_error(self):
        @task
        def task_a(a):
            return a

        @task
        def task_b(a):
            return a

        @pipeline
        def task_circle():
            a = task_a(1)
            b = task_b(a)
            b.task.set_downstream(a)
            return b

        with new_dbnd_context(conf={"core": {"recheck_circle_dependencies": "True"}}):
            with pytest.raises(
                DatabandBuildError, match="A cyclic dependency occurred"
            ):
                task_circle.task()

        with pytest.raises(DatabandRunError, match="A cyclic dependency occurred"):
            task_circle.dbnd_run()

    def test_partitioned_band(self):
        """
        Use-case when we create a pipeline of partitions
        and we have a task that want to use root of that partitions"

        root/p1 ->  task_ttask_that_use_root(root)
        root/p2

        """

        @task
        def tpartition_task(partition_id=parameter[int]):
            return partition_id

        @pipeline
        def pipeline_with_partitions(num_of_paritions=parameter[int]):
            partitions = {}
            root_dir = "/tmp/1"
            for i in range(num_of_paritions):
                partitions[str(i)] = tpartition_task(
                    partition_id=i, result=os.path.join(root_dir, "p=%s" % i)
                )
            # root dir "target" created inside this pipeline
            # every "str" converted into "target" will be binded to the "owning" pipeline
            # meaning -> root_dir will become target(root_dir).task=self
            # automatically, every task that will use it will "wait" for "this pipeline" to complete
            return root_dir, partitions

        @task
        def ttask_that_use_root(root_dir: PathStr):
            return root_dir

        @pipeline
        def main_pipeline(num_of_paritions=parameter[int]):
            root_dir, partitions = pipeline_with_partitions(
                num_of_paritions=num_of_paritions
            )
            return ttask_that_use_root(root_dir)

        main_pipeline_instance = main_pipeline.t(3)
        main_pipeline_instance.ctrl.describe_dag.describe_dag()
        main_pipeline_instance.dbnd_run()
        task_that_use_root_instance = main_pipeline_instance.result  # type:Task

        print(task_that_use_root_instance)


@task
def t1_producer(a_str):
    # type: (str)->(str, datetime.datetime, datetime.timedelta, int)

    now = datetime.datetime.now()
    return ["strX", now, datetime.timedelta(seconds=1), 1]


@task
def t1_consumer(a_str, b_datetime, c_timedelta, d_int):
    # type: (str, datetime.datetime, datetime.timedelta, int) -> DataFrame
    return pd.DataFrame(data=[[1, 1]], columns=["c1", "c2"])


@band
def t1_producer_consumer():
    tp = t1_producer("1")
    tc = t1_consumer(*tp)
    return tc


@task
def t2_df(a=1):
    # type: (...)-> pd.DataFrame
    return pd.DataFrame(data=[[a, a]], columns=["c1", "c2"])


######
# Path


@task
def t2_path_list(list_of_paths):
    # type: (List[Path]) -> object

    for l in list_of_paths:
        assert isinstance(l, Path)
    return list_of_paths[-1]


@band
def t2_band_paths():
    ll = [t2_df(i) for i in range(2)]
    return t2_path_list(ll)


###
# DataFrame


@task
def t2_df_list(list_of_df):
    # type: (List[pd.DataFrame]) -> pd.DataFrame

    for l in list_of_df:
        assert isinstance(l, pd.DataFrame)
    return list_of_df[-1]


@band
def t2_band_df():
    ll = [t2_df(i) for i in range(2)]
    return t2_df_list(ll)


######
# Dict[str, Path]


@task
def t2_df_dict(dict_of_df):
    # type: (Dict[str, pd.DataFrame]) -> pd.DataFrame

    for l in dict_of_df.values():
        assert isinstance(l, pd.DataFrame)
    return list(dict_of_df.values())[-1]


@band
def t2_band_dict_df():
    ll = {str(i): t2_df(i) for i in range(2)}
    return t2_df_dict(ll)


class TestTaskRuntimeData(object):
    def test_simple_types(self):
        t1_producer_consumer.dbnd_run()

    def test_path_types(self):
        t2_band_paths.dbnd_run()

    def test_df_types(self):
        t2_band_df.dbnd_run()

    def test_df_dict(self):
        t2_band_dict_df.dbnd_run()
