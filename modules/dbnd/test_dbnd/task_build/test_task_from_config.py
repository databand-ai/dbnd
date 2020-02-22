import io

from pytest import fixture

from dbnd import config
from dbnd._core.configuration.config_readers import read_from_config_stream
from dbnd._core.task_build.task_registry import build_task_from_config


test_config = """
[my_t]
_type=local_machine

[my_tt]
_from=my_t

[my_ttt_from_tt]
_from=my_tt

"""


class TestTaskFromConfig(object):
    @fixture(autouse=True)
    def load_test_config(self):
        ts = read_from_config_stream(io.StringIO(test_config))
        with config(ts):
            yield ts

    def test_simple_config(self):
        actual = build_task_from_config("my_t")
        assert actual.get_task_family() == "local_machine"
        assert actual.task_name == "my_t"

    def test_config_with_from(self):
        actual = build_task_from_config("my_tt")
        assert actual.get_task_family() == "local_machine"
        assert actual.task_name == "my_tt"

    def test_config_with_double_from(self):
        actual = build_task_from_config("my_ttt_from_tt")
        assert actual.get_task_family() == "local_machine"
        assert actual.task_name == "my_ttt_from_tt"
