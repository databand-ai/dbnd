# © Copyright Databand.ai, an IBM Company 2022

from __future__ import absolute_import

import logging
import zlib

from typing import List

import pandas as pd
import pytest
import six

from dbnd import new_dbnd_context, output, override, parameter, pipeline, task
from dbnd._core.constants import TaskExecutorType
from dbnd._core.settings import CoreConfig
from dbnd._core.utils.seven import cloudpickle
from dbnd.orchestration.run_executor.run_executor import RunExecutor
from dbnd.orchestration.run_settings import RunConfig
from dbnd.tasks import PythonTask
from dbnd_test_scenarios.test_common.task.factories import TTask


logger = logging.getLogger(__name__)


# comment out for fixture "benchmark" via pytest-benchmark
@pytest.fixture
def benchmark(cProfile_benchmark):
    return cProfile_benchmark


@task
def ttask_g(tparam="1", tidx=0):
    # type:(str)->str
    return "result %s" % tparam


def generate_huge_task(num_of_tasks):
    # return chain of tasks 1->2->3->.. ->N
    cur_t = "start"
    for i in range(num_of_tasks):
        cur_t = ttask_g.task(tparam=cur_t, tidx=i)
    return cur_t


class TClassTask(PythonTask):
    t_param = parameter.value("1")
    t_output = output.data

    def run(self):
        self.t_output.write("%s" % self.t_param)


@task
def ttask_dataframe_generic(tparam=1, t_list=None):
    # type:(int, List[str])->pd.DataFrame
    return pd.DataFrame(data=[[tparam, tparam]], columns=["c1", "c2"])


@pipeline
def pipeline_with_generics(selected_partners):
    # type: (List[int])-> List[pd.DataFrame]
    partner_data = [
        ttask_dataframe_generic(tparam=partner, t_list=["a", "b"])
        for partner in selected_partners
    ]
    return partner_data


class TestRunPickle(object):
    def _save_graph(self, task):
        with new_dbnd_context(
            conf={
                RunConfig.task_executor_type: override(TaskExecutorType.local),
                RunConfig.dry: override(True),
                CoreConfig.tracker: override(["console"]),
            }
        ) as dc:
            run = dc.dbnd_run_task(task_or_task_name=task)
            run.run_executor.save_run_pickle()

        loaded_run = RunExecutor.load_run(
            dump_file=run.run_executor.driver_dump, disable_tracking_api=False
        )
        assert loaded_run
        return run

    def test_save_databand_run(self):
        s = TTask()
        r = self._save_graph(s)

        actual = RunExecutor.load_run(r.run_executor.driver_dump, False)
        assert actual

    def test_save_huge_graph(self):
        # let validate that pickle python2 can handle huge graph of tasks
        task = generate_huge_task(200)

        # dill._dill._trace(True)
        r = self._save_graph(task)
        actual = RunExecutor.load_run(r.run_executor.driver_dump, False)
        assert actual

    def _benchmark_pipeline_save(
        self, benchmark, pipeline, pickle_func=cloudpickle.dumps
    ):
        r = self._save_graph(pipeline)

        result = benchmark(pickle_func, r)
        logger.warning("Pickeled result size %s", len(result))
        logger.warning("zlib result size %s", len(zlib.compress(result, 9)))

    def test_save_time_1(self, benchmark):
        s = TTask()
        self._benchmark_pipeline_save(benchmark=benchmark, pipeline=s)

    def test_dump_dagrun_simple(self):
        s = TClassTask()
        self._save_graph(s)

    @pytest.mark.skipif(not six.PY3, reason="fails on generics at tox")
    def test_dump_dagrun_generics1(self):
        # this test fails if run from tox (all tests) with python2
        # old version of cloudpickle doesn't support generics cache at ABC
        # some tests from the test suite has dynamic tasks
        # this test fails if runs after that ( pickling generics )
        s = pipeline_with_generics.task([1, 2])
        self._save_graph(s)

    #
    # def test_save_time_2(self, benchmark):
    #     s = TTask()
    #     self._benchmark_pipeline_save(benchmark=benchmark, pipeline=s)
    #
    # def test_save_time_big_fast(self, benchmark):
    #     s = PredictWineQualityParameterSearch()
    #     self._benchmark_pipeline_save(benchmark=benchmark, pipeline=s, pickle_func=fast_pickle.dumps)
    #
    # def test_save_time_big_cloud(self, benchmark):
    #     s = PredictWineQualityParameterSearch()
    #     self._benchmark_pipeline_save(benchmark=benchmark, pipeline=s, pickle_func=cloudpickle.dumps)
    #
    # def test_save_time_11(self, benchmark):
    #     s = PredictWineQualityParameterSearch()
    #     result = benchmark(fast_pickle.dumps, s)
    #     logger.info("Size %s", len(result))
    #
    # def test_save_time_13(self, benchmark):
    #     s = TTask()
    #     benchmark(pickle.dumps, obj=s.task_schema)
