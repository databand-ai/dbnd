# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd import Config, dbnd_config, new_dbnd_context, override, parameter, task
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.task_build.task_context import TaskContextPhase
from dbnd._core.task_build.task_registry import build_task_from_config
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.utils.seven import qualname_func


def dynamically_calculated():
    logging.error("Calculating!")
    return "test_f_value"


class DummyConfig(Config):
    _conf__task_family = "my_dummy"

    foo = parameter(default="foofoo")[str]
    bar = parameter(default="barbar")[str]


class TConfig(Config):
    _conf__task_family = "tconfig"
    config_value_s1 = parameter[str]
    config_value_s2 = parameter[str]


@task(
    task_config=dict(
        tconfig=dict(
            config_value_s1=override("override_config_s1"),
            config_value_s2="task_config_regular_s2",
        )
    )
)
def dummy_nested_config_task(config_name):
    # type: ( str)-> object
    actual = build_task_from_config(task_name=config_name)
    return (actual.config_value_s1, actual.config_value_s2)


class TestTaskConfig(object):
    def test_task_config_override(self):
        dbnd_config.log_layers()
        actual = dummy_nested_config_task.dbnd_run(config_name="tconfig")
        assert actual.task.result.load(object) == (
            "override_config_s1",
            "task_config_regular_s2",
        )

    def test_task_sub_config_override(self):
        actual = dummy_nested_config_task.dbnd_run(config_name="sub_tconfig")
        # override_config_s1  -- we use override -affect all configs ( derived also)
        # task_config at t applied on top of config files ->
        # sub_tconfig is "_from" tconfig-> we get task_config_regular_s2
        assert actual.task.result.load(object) == (
            "override_config_s1",
            "task_config_regular_s2",
        )

    def test_task_runner_context(self):
        # same as test_task_sub_config_override
        # we check that task_run_context "put" us in the right config layer
        actual = dummy_nested_config_task.dbnd_run(config_name="sub_tconfig")
        with DatabandRun.context(actual):
            task_run = actual.task.current_task_run  # type: TaskRun

            with task_run.task.ctrl.task_context(phase=TaskContextPhase.BUILD):
                actual = build_task_from_config(task_name="sub_tconfig")
                assert actual.config_value_s1 == "override_config_s1"
                # because we have task_config in dummy_nested_config_task that overrides config
                # tconfig is higher than  value for [ sub_tconfig] at config file
                # config layer is down..
                assert actual.config_value_s2 == "task_config_regular_s2"

    def test_dynamically_calcualted_config(self):
        @task(
            task_config={
                "t_config": {
                    "config_value_s1": "@python://%s"
                    % qualname_func(dynamically_calculated)
                }
            }
        )
        def t_f():
            return dummy_nested_config_task(config_name="tconfig")

        actual = t_f.dbnd_run()
        assert actual.task.result.load(object) == (
            "override_config_s1",
            "task_config_regular_s2",
        )

    def test_single_instance(self):
        with new_dbnd_context(name="first") as ctx:
            config1 = ctx.settings.get_config("my_dummy")
            config2 = Config.from_databand_context("my_dummy")
            config3 = DummyConfig.from_databand_context()
            config4 = ctx.settings.get_config("my_dummy")
            assert config1 is config2
            assert config1 is config3
            assert config1 is config4

            config1.foo = "123"

            assert config2.foo == "123"
            assert config3.foo == "123"
            assert config4.foo == "123"

    def test_different_instances(self):
        with new_dbnd_context(name="first") as ctx:
            config1 = ctx.settings.get_config("my_dummy")
            with new_dbnd_context(name="second") as ctx2:
                config2 = ctx2.settings.get_config("my_dummy")
                assert config1 is not config2
