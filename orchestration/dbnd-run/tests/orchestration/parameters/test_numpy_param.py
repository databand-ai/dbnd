# © Copyright Databand.ai, an IBM Company 2022

import logging

import numpy as np

from pytest import fixture

from dbnd import PythonTask, output, parameter, task
from dbnd_run.testing.helpers import assert_run_task
from targets import target


logger = logging.getLogger(__name__)


class TestNumpyTask(object):
    @fixture
    def numpy_array(self):
        return np.array([1, 2, 3, 4])

    @fixture
    def numpy_array_on_disk(self, tmpdir, numpy_array):
        t = target(str(tmpdir.join("numpy.npy")))
        np.save(t.path, numpy_array)
        return t

    def test_numpy_task(self, numpy_array, numpy_array_on_disk):
        class NumpyTask(PythonTask):
            p_input = parameter[np.ndarray]
            p_output = output.file_numpy.target

            def run(self):
                logger.info("->%s", self.p_input)

                self.p_output.write_numpy_array(self.p_input)

        numpy_task = NumpyTask(p_input=numpy_array_on_disk.path)
        assert numpy_task.p_output.path.endswith(".npy")
        assert_run_task(numpy_task)

        actual = np.load(numpy_task.p_output.path)
        logger.info("task ->%s", actual)
        np.testing.assert_array_equal(actual, numpy_array)

    def test_numpy_decorator(self, numpy_array, numpy_array_on_disk):
        @task
        def numpy_inline(p_input):
            # type: (np.ndarray) -> np.ndarray
            logger.info("->%s", p_input)
            return p_input

        inline_task = numpy_inline.t(numpy_array_on_disk.path)
        assert_run_task(inline_task)

        actual = np.load(inline_task.result.path)
        logger.info("inline ->%s", actual)
        np.testing.assert_array_equal(actual, numpy_array)
