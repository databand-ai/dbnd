from __future__ import absolute_import

import logging
import zlib

from typing import List

import pandas as pd
import pytest

from dbnd import new_dbnd_context, output, override, parameter, pipeline, task
from dbnd._core.constants import TaskExecutorType
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.settings import CoreConfig, RunConfig
from dbnd._vendor.cloudpickle import cloudpickle
from dbnd.tasks import PythonTask
from dbnd.testing.helpers import initialized_run
from test_dbnd.factories import TTask


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
                CoreConfig.tracker: override(["console"]),
            }
        ) as dc:
            run = dc.dbnd_run_task(task_or_task_name=task)
            run.save_run()

        loaded_run = DatabandRun.load_run(
            dump_file=run.driver_dump, disable_tracking_api=False
        )
        assert loaded_run
        return run

    def test_save_databand_run(self):
        s = TTask()
        r = self._save_graph(s)

        actual = DatabandRun.load_run(r.driver_dump, False)
        assert actual

    def test_save_huge_graph(self):
        # let validate that pickle python2 can handle huge graph of tasks
        task = generate_huge_task(200)

        # dill._dill._trace(True)
        r = self._save_graph(task)
        actual = DatabandRun.load_run(r.driver_dump, False)
        assert actual

    def _benchmark_pipeline_save(
        self, benchmark, pipeline, pickle_func=cloudpickle.dumps
    ):
        with initialized_run(task_or_task_name=pipeline) as r:  # type: DatabandRun
            logger.warning("starting benchmark")
            result = benchmark(pickle_func, r)
        logger.warning("Pickeled result size %s", len(result))
        logger.warning("zlib result size %s", len(zlib.compress(result, 9)))

    def test_save_time_1(self, benchmark):
        s = TTask()
        self._benchmark_pipeline_save(benchmark=benchmark, pipeline=s)

    def test_dump_dagrun_simple(self):
        s = TClassTask()
        self._save_graph(s)

    def test_dump_dagrun_generics1(self):
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
