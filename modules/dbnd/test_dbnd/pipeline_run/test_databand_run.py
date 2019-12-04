import logging
import zlib

import pytest

from cloudpickle import cloudpickle
from dbnd import new_dbnd_context, override, task
from dbnd._core.constants import TaskExecutorType
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.settings import CoreConfig
from dbnd._core.settings.engine import LocalMachineEngineConfig
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


class TestDatabandRun(object):
    def _save_graph(self, task):
        with new_dbnd_context(
            conf={
                LocalMachineEngineConfig.task_executor_type: override(
                    TaskExecutorType.local
                ),
                CoreConfig.tracker: override(["console"]),
            }
        ) as dc:
            task_run = dc.dbnd_run_task(task_or_task_name=task)
            task_run.save_run()
        return task_run

    def test_save_databand_run(self):
        s = TTask()
        r = self._save_graph(s)

        actual = DatabandRun.load_run(r.driver_dump)
        assert actual

    def test_save_huge_graph(self):
        # let validate that pickle python2 can handle huge graph of tasks
        task = generate_huge_task(200)

        # dill._dill._trace(True)
        r = self._save_graph(task)
        actual = DatabandRun.load_run(r.driver_dump)
        assert actual

    def _benchmark_pipeline_save(
        self, benchmark, pipeline, pickle_func=cloudpickle.dumps
    ):
        with initialized_run(task_or_task_name=pipeline) as r:  # type: DatabandRun
            with r.task_executor.prepare_run():
                logger.warning("starting benchmark")
                result = benchmark(pickle_func, r)
        logger.warning("Pickeled result size %s", len(result))
        logger.warning("zlib result size %s", len(zlib.compress(result, 9)))

    def test_save_time_1(self, benchmark):
        s = TTask()
        self._benchmark_pipeline_save(benchmark=benchmark, pipeline=s)

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
