import os

from dbnd import config
from dbnd._core.configuration.config_readers import (
    read_environ_config,
    read_from_config_files,
)
from test_dbnd.scenarios import scenario_path


class TestConfigReaders(object):
    def test_read_(self):
        actual = read_from_config_files(
            [scenario_path("config_files", "test_config_reader.cfg")]
        )
        with config(actual):
            assert (
                config.get("test_config_reader", "test_config_reader") == "test_value"
            )

    def test_read_environ_config(self):
        os.environ["DBND__TEST_SECTION__TEST_KEY"] = "TEST_VALUE"
        actual = read_environ_config()
        with config(actual):
            assert config.get("test_section", "test_key") == "TEST_VALUE"
