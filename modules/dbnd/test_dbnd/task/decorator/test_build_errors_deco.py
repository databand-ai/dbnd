import datetime

import pytest

from dbnd import parameter, task
from dbnd._core.errors import DatabandBuildError, DatabandError


class TestBuildErrorsDecorators(object):
    def test_no_type(self):
        @task
        def err_f_no_type(a):
            return a

        err_f_no_type.dbnd_run(1)

    def test_wrong_type(self):
        @task
        def err_f_wrong_type(a):
            # type: () -> str
            return str(a)

        err_f_wrong_type.dbnd_run(1)

    def test_comment_first(self):
        @task
        def err_f_comment_first(a):
            # sss
            # type: (datetime.datetime)->str
            assert isinstance(a, datetime.datetime)
            return "OK"

        err_f_comment_first.dbnd_run("2018-01-01")

    def test_missing_params(self):
        @task
        def err_f_missing_params(a, b, c):
            # sss
            # type: (str) -> str
            return a

        err_f_missing_params.dbnd_run(1, 2, 3)

    def test_unknown_return_type(self):
        @task
        def err_f_unknown_return_type(a):
            # type: (str) -> err_f_missing_params
            return a

        err_f_unknown_return_type.dbnd_run(1)

    def test_double_definition(self):
        with pytest.raises(DatabandBuildError):

            @task(a=parameter[int])
            def err_f_unknown_return_type(a=parameter[str]):
                # type: (str) -> None
                return a

            err_f_unknown_return_type()  # ???
