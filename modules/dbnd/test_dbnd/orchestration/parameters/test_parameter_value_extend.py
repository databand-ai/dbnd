import pytest

from dbnd import Config, config, override, parameter, task
from dbnd._core.configuration.config_value import extend


class DummyConfig(Config):
    _conf__task_family = "dummy"
    dict_config = parameter(default={"default": "default"})[dict]
    list_config = parameter(default=[1])[list]
    str_config = parameter(default="32")[str]
    int_config = parameter(default=12)[int]


# test_config = """
# [dummy]
# dict_config = {"first_key": "basic_value"}
# list_config = ["basic_value"]
# str_config = six
# int_config = 6
#
# [dummy_1]
# _type = dummy
# list_config = ["env_1_value"]
#
#
# [dummy_2]
# _type = dummy
# dict_config = {"second_key": "env_2_value"}
# """


@task
def example_task(name, expected_dict):
    assert Config.from_databand_context(name).dict_config == expected_dict


@task(task_config={"dummy": {"dict_config": {"A": "a"}}})
def example_task_with_task_config(expected_dict):
    assert DummyConfig.from_databand_context().dict_config == expected_dict


@task(task_config={"dummy": {"dict_config": override({"A": "a"})}})
def example_task_with_task_config_override(expected_dict):
    assert DummyConfig.from_databand_context().dict_config == expected_dict


@task(task_config={"dummy": {"dict_config": extend({"new_key": "value"})}})
def example_task_with_task_config_extend(expected_dict):
    assert DummyConfig.from_databand_context().dict_config == expected_dict


class TestBuildTaskWithMergeOperator(object):
    @pytest.fixture
    def config_sections(self):
        return {
            DummyConfig.dict_config: {"first_key": "basic_value"},
            # DummyConfig.list_config: ["basic_value"],
            # DummyConfig.str_config: "six",
            # DummyConfig.int_config: 6,
        }

    def test_with_task(self, config_sections):
        with config(config_sections):
            example_task.task(
                name="dummy", expected_dict={"first_key": "basic_value"}
            ).dbnd_run()

    def test_with_task_class_only(self, config_sections):
        with config(config_sections):
            example_task_with_task_config.task(expected_dict={"A": "a"}).dbnd_run()

    def test_with_task_class_and_kwarg_override(self, config_sections):
        with config(config_sections):
            example_task_with_task_config.task(
                override={"dummy": {"dict_config": {"A": "b"}}},
                expected_dict={"A": "b"},
            ).dbnd_run()

    def test_with_task_class_override(self, config_sections):
        with config(config_sections):
            example_task_with_task_config_override.task(
                expected_dict={"A": "a"}
            ).dbnd_run()

    def test_with_task_class_override_and_kwarg_override(self, config_sections):
        with config(config_sections):
            example_task_with_task_config_override.task(
                override={"dummy": {"dict_config": {"A": "b"}}},
                expected_dict={"A": "b"},  # user override - highest priority
            ).dbnd_run()

    def test_with_task_config_extend(self, config_sections):
        with config(config_sections):
            example_task_with_task_config_extend.task(
                expected_dict={"new_key": "value", "first_key": "basic_value"}
            ).dbnd_run()

    def test_with_task_class_extend_and_kwarg_override(self, config_sections):
        with config(config_sections):
            example_task_with_task_config_extend.task(
                override={"dummy": {"dict_config": {"A": "b"}}},
                expected_dict={"A": "b"},
            ).dbnd_run()

    def test_with_task_class_extend_and_override_previous_layer(self, config_sections):
        with config({DummyConfig.dict_config: override({"override": "win"})}):
            with config(config_sections):
                example_task_with_task_config_extend.task(
                    expected_dict={"override": "win"}
                ).dbnd_run()

    def test_with_task_class_extend_and_override_layer(self, config_sections):
        with config(config_sections):
            with config({DummyConfig.dict_config: override({"override": "win"})}):
                # can't merge into override value
                example_task_with_task_config_extend.task(
                    expected_dict={"override": "win"}
                ).dbnd_run()
