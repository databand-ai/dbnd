import mock
import pytest

from dbnd._core.decorator.dbnd_func_proxy import task
from dbnd._core.errors import UnknownParameterError
from dbnd._core.errors.base import DatabandRunError
from dbnd._core.failures import dbnd_handle_errors


@task
def func_you_want_to_marry_with(denominator=1):
    dividing_by_zero_is_fun = 42 / denominator
    return "We'll definitely get here: %s" % dividing_by_zero_is_fun


class TestErrorHandling(object):
    def test_build_error_handling_exit(self):
        # Bad parameter, error on build
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            with dbnd_handle_errors(True):
                # Error will cause exit
                func_you_want_to_marry_with.task(bad_param="This param does not exist")

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

    def test_build_error_handling_no_exit(self):
        # Bad parameter, error on build
        with pytest.raises(UnknownParameterError):
            with dbnd_handle_errors(exit_on_error=False):
                # Error won't cause exit
                func_you_want_to_marry_with.task(bad_param="This param does not exist")

    def test_run_error_handling_exit(self):
        # Good parameter, error in runtime while dividing by zero
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            with dbnd_handle_errors(True):
                # Error will cause exit
                func_you_want_to_marry_with.dbnd_run(denominator=0)

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

    def test_run_error_handling_no_exit(self):
        # Good parameter, error in runtime while dividing by zero
        with pytest.raises(DatabandRunError):
            with dbnd_handle_errors(exit_on_error=False):
                # Error won't cause exit
                func_you_want_to_marry_with.dbnd_run(denominator=0)

    def test_no_double_handling(self, caplog):
        # Good parameter, error in runtime while dividing by zero

        with pytest.raises(DatabandRunError):
            with dbnd_handle_errors(exit_on_error=False):
                # Error won't cause exit
                func_you_want_to_marry_with.dbnd_run(denominator=0)
        assert caplog.text.count("Your run has failed! See more info above.") == 1
