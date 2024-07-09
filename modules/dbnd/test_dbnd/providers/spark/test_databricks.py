# © Copyright Databand.ai, an IBM Company 2024

import pytest

from dbnd.providers.spark.dbnd_spark_init import is_databricks_notebook_env


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
