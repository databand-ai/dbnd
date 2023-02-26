# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd import new_dbnd_context
from dbnd._core.errors import UnknownParameterError
from dbnd._core.errors.base import DatabandRunError
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.settings import LoggingConfig
from dbnd._core.task_build.dbnd_decorator import task


@task
def func_with_error(denominator=1):
    dividing_by_zero_is_fun = 42 / denominator
    return "We'll definitely get here: %s" % dividing_by_zero_is_fun


class TestErrorHandling(object):
    def test_build_error_handling_exit(self):
        # Bad parameter, error on build
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            with dbnd_handle_errors(True):
                # Error will cause exit
                func_with_error.task(bad_param="This param does not exist")

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

    def test_build_error_handling_no_exit(self):
        # Bad parameter, error on build
        with pytest.raises(UnknownParameterError):
            with dbnd_handle_errors(exit_on_error=False):
                # Error won't cause exit
                func_with_error.task(bad_param="This param does not exist")

    def test_run_error_handling_exit(self):
        # Good parameter, error in runtime while dividing by zero
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            with dbnd_handle_errors(True):
                # Error will cause exit
                func_with_error.dbnd_run(denominator=0)

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

    def test_run_error_handling_no_exit(self):
        # Good parameter, error in runtime while dividing by zero
        with pytest.raises(DatabandRunError):
            with dbnd_handle_errors(exit_on_error=False):
                # Error won't cause exit
                func_with_error.dbnd_run(denominator=0)

    def test_no_double_handling(self, caplog):
        # Good parameter, error in runtime while dividing by zero

        # if we use dbnd logging, caplog will not work as it was initialized before our setup
        with new_dbnd_context(conf={LoggingConfig.disabled: True}):
            with pytest.raises(DatabandRunError):
                with dbnd_handle_errors(exit_on_error=False):
                    # Error won't cause exit
                    func_with_error.dbnd_run(denominator=0)
        assert (
            caplog.text.count("Your run has failed! See more info above.") == 1
        ), f"LOG: --------\n\n\n\n {caplog.text}\n----LOG END--"
