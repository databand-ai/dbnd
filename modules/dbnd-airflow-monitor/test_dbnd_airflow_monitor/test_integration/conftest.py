# conftest.py
import pytest


try:
    from dbnd_web.utils.testing.dbnd_web_testing.utils import WebAppTest

    pytest_plugins = [
        "dbnd.testing.pytest_dbnd_plugin",
        "dbnd.testing.pytest_dbnd_markers_plugin",
        "dbnd.testing.pytest_dbnd_home_plugin",
        "dbnd_web.utils.testing.dbnd_web_testing.pytest_web_plugin",
    ]
except ModuleNotFoundError:
    pytest_plugins = []

    class WebAppTest(object):
        pass

    @pytest.fixture(autouse=True, scope="module")
    def check_dbnd_web():
        pytest.skip("skipped due to missing dbnd_web")
