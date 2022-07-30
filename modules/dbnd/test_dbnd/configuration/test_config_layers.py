# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.configuration.pprint_config import pformat_current_config


def _tc(key, value):
    return {"test_section": {key: value}}


def _a():
    return "from_a"


class TestDbndConfig(object):
    def test_log_current_config(self):
        with config(
            _tc("test_log_current_config_abc", "test_log_current_config_value"),
            source="test_source",
        ):
            config.log_current_config()

    def test_pformat_current_config(self):
        with config(
            _tc("test_log_current_config_abc", "test_log_current_config_value"),
            source="test_source",
        ):
            actual = pformat_current_config(config)
            assert "test_log_current_config_abc" in actual
            assert "test_log_current_config_value" in actual

    def test_pformat_table_current_config(self):
        with config(
            _tc("test_log_current_config_abc", "test_log_current_config_value"),
            source="test_source",
        ):
            actual = pformat_current_config(
                config, as_table=True, sections=["test_section"]
            )
            assert "test_log_current_config_abc" in actual
            assert "test_log_current_config_value" in actual
            assert "test_source" in actual

    def test_layers(self):
        with config({"b": dict(a=2)}):
            config.log_current_config()

            config.set("core", "a", "1")
            config.set("core", "b", "1")

            with config({"core": dict(a=5)}):
                config.log_current_config(as_table=True)
                assert config.get("core", "a") == 5

            config.log_current_config()
            config.log_layers()

    def test_str_interpolation(self):
        with config(
            {
                "b": dict(
                    a="@python://%s" % "test_dbnd.configuration.test_config_layers._a"
                )
            }
        ):
            assert config.get("b", "a") == "from_a"
