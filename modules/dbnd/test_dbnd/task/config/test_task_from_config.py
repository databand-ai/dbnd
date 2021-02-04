from pytest import fixture

from dbnd import Config, config, parameter
from dbnd._core.configuration.config_readers import read_from_config_stream
from dbnd._core.task_build.task_registry import build_task_from_config
from dbnd._core.utils import seven


class MyTaskConfig(Config):
    p_str = parameter[str]
    p_basic = parameter[str]
    p_advanced = parameter(default="default")[str]


test_config = """

[my_t]
_type=test_dbnd.task.config.test_task_from_config.MyTaskConfig
p_str = my_t_sql
p_basic = basic_my_t


[my_tt]
_from=my_t
p_str=my_tt_sql

[my_ttt_from_tt]
_from=my_tt
p_str=my_ttt_from_tt_sql
"""


class TestTaskFromConfig(object):
    @fixture(autouse=True)
    def load_test_config(self):
        ts = read_from_config_stream(seven.StringIO(test_config))
        with config(ts):
            yield ts

    def test_simple_config(self):
        actual = build_task_from_config("my_t")
        assert actual.get_task_family() == "MyTaskConfig"
        assert actual.task_name == "my_t"
        assert actual.p_str == "my_t_sql"
        assert actual.p_basic == "basic_my_t"

    def test_config_with_from(self):
        actual = build_task_from_config("my_tt")
        assert actual.get_task_family() == "MyTaskConfig"
        assert actual.task_name == "my_tt"
        assert actual.p_str == "my_tt_sql"

    def test_config_with_double_from(self):
        actual = build_task_from_config("my_ttt_from_tt")
        assert actual.get_task_family() == "MyTaskConfig"
        assert actual.task_name == "my_ttt_from_tt"
        assert actual.p_str == "my_ttt_from_tt_sql"
        assert actual.p_basic == "basic_my_t"
