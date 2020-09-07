import logging

import pytest

from dbnd import PipelineTask, parameter
from dbnd._core.errors import (
    DatabandBuildError,
    MissingParameterError,
    UnknownParameterError,
)
from dbnd_test_scenarios.test_common.task.factories import TTask


logger = logging.getLogger(__name__)


class A(TTask):
    p = parameter[int]


class ForgotParam(TTask):
    param = parameter[str]

    def run(self):
        pass


class TestTaskBuildErrors(object):
    def test_missing_param(self):
        def create_a():
            return A()

        with pytest.raises(MissingParameterError):
            create_a()

    def test_multiple_missing_param(self):
        class TMissingParamsMultiple(TTask):
            p1 = parameter[int]
            p2 = parameter[int]
            p3 = parameter[int]

        with pytest.raises(DatabandBuildError, message="Failed to create task"):
            TMissingParamsMultiple()

    def test_unknown_param_root(self):
        with pytest.raises(UnknownParameterError):
            A(p=5, q=4)

    def test_unknown_param(self):
        class MyPTWithError(PipelineTask):
            def band(self):
                A(p=5, q=4)

        with pytest.raises(DatabandBuildError) as exc_info:
            MyPTWithError()

        logger.error(exc_info.value.help_msg)
        assert "MyPTWithError" in exc_info.value.help_msg

    def test_local_insignificant_param(self):
        """ Ensure we have the same behavior as in before a78338c  """

        class MyTask(TTask):
            # This could typically be "--num-threads=True"
            x = parameter(significant=False)[str]

        MyTask(x="arg")
        with pytest.raises(MissingParameterError):
            MyTask()

    def test_forgot_param(self):
        with pytest.raises(MissingParameterError):
            ForgotParam()

    def test_no_value_type(self):
        with pytest.raises(DatabandBuildError, message="doesn't have type"):

            class MyPTWithError(PipelineTask):
                wrong_param = parameter

                def band(self):
                    return None

    def test_no_value_from_default(self):
        with pytest.raises(DatabandBuildError, message="has default value"):

            class MyT(object):
                pass

            class MyPTWithError(PipelineTask):
                wrong_param = parameter.value(MyT)

                def band(self):
                    return None

    def test_subtype_not_in_simpletype(self):
        with pytest.raises(
            DatabandBuildError, message="is not supported by main value"
        ):

            class MyPTWithError(PipelineTask):
                wrong_param = parameter.sub_type(bool)[int]

                def band(self):
                    return None

    def test_band_param(self):
        class MyPTWithBandError(PipelineTask):
            def band(self):
                raise Exception("User exception in band")

        with pytest.raises(DatabandBuildError) as exc_info:
            MyPTWithBandError()

        logger.error("MESSAGE: %s", exc_info.value)
        assert "User exception in band" in str(exc_info.value)
