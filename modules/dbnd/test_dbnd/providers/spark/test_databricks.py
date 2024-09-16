# © Copyright Databand.ai, an IBM Company 2024

import pytest

from dbnd._core.utils.basics.path_utils import relative_path
from dbnd.providers.spark.dbnd_databricks import is_databricks_notebook_env
from dbnd.testing.helpers import run_test_notebook


@pytest.fixture
def set_databand_only_console_tracker(monkeypatch):
    try:
        monkeypatch.setenv("DBND__CORE__TRACKER", "console")
        yield
    finally:
        monkeypatch.delenv("DBND__CORE__TRACKER", raising=False)


@pytest.fixture
def set_databricks_env_var(monkeypatch):
    try:
        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.0")
        yield
    finally:
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)


class TestDataBricksNoteBook(object):
    def test_is_databricks_env_present(self, set_databricks_env_var):
        result = is_databricks_notebook_env()
        assert result is True

    def test_is_databricks_env_not_present(self):
        result = is_databricks_notebook_env()
        assert result is False

    def test_spark_notebook_success(self, set_databand_only_console_tracker):
        path = relative_path(__file__, "jupyter/Databand_TestBook_1_Success.ipynb")
        nb, errors = run_test_notebook(path)
        assert errors == []

    def test_spark_notebook_fail(self, set_databand_only_console_tracker):
        path = relative_path(__file__, "jupyter/Databand_TestBook_2_Fail.ipynb")
        nb, errors = run_test_notebook(path)

        assert errors != []
        assert len(errors) == 1
        error = errors.pop()
        assert error.ename == "Exception"
        assert error.evalue == "Test exception"
