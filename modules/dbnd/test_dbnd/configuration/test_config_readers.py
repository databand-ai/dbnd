# © Copyright Databand.ai, an IBM Company 2022

import os

from dbnd import config
from dbnd._core.configuration.config_readers import (
    read_environ_config,
    read_from_config_file,
    read_from_config_files,
)
from dbnd_test_scenarios import scenario_path, scenario_target


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

    def test_reading_from_zip(self):
        zip_file_target = scenario_target(
            "dbnd_zip_file", "dbnd.zip", "dbnd", "conf", "databand-core.cfg"
        )
        result_config = read_from_config_file(zip_file_target)
        assert result_config is not None
        assert result_config.get("core") is not None
        assert result_config.get("core").get("tracker") is not None
        assert result_config.get("core").get("tracker").value is not None
