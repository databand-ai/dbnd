import logging

from dbnd import override, task
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.task_build.task_registry import build_task_from_config
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.utils.seven import qualname_func


def dynamically_calculated():
    logging.error("Calculating!")
    return "test_f_value"


@task(
    task_config=dict(
        tconfig=dict(
            config_value_s1=override("override_config_s1"),
            config_value_s2="regular_config_s2",
        )
    )
)
def dummy_nested_config_task(config_name):
    # type: ( str)-> object
    actual = build_task_from_config(task_name=config_name)
    return (actual.config_value_s1, actual.config_value_s2)


class TestTaskConfig(object):
    def test_task_config_override(self):
        actual = dummy_nested_config_task.dbnd_run(config_name="tconfig")
        assert actual.task.result.load(object) == (
            "override_config_s1",
            "regular_config_s2",
        )

    def test_task_sub_config_override(self):
        actual = dummy_nested_config_task.dbnd_run(config_name="sub_tconfig")
        # we get "value_sub_from_databand_test_cfg_s2" as we are using sub_tconfig
        # so task_config overrides only basic "tconfig"
        # In this example:
        #   override_config_s1  -- we use override -affect all configs ( derived also)
        #   value_sub_from_databand_test_cfg_s2 -- nothing overrides it
        assert actual.task.result.load(object) == (
            "override_config_s1",
            "value_sub_from_databand_test_cfg_s2",
        )

    def test_task_runner_context(self):
        # same as test_task_sub_config_override
        # we check that task_run_driver_context "put" us in the right config layer
        actual = dummy_nested_config_task.dbnd_run(config_name="sub_tconfig")
        with DatabandRun.context(actual):
            task_run = actual.task.current_task_run  # type: TaskRun

            with task_run.runner.task_run_driver_context():
                actual = build_task_from_config(task_name="sub_tconfig")
                assert actual.config_value_s1 == "override_config_s1"
                assert actual.config_value_s2 == "value_sub_from_databand_test_cfg_s2"

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
            "regular_config_s2",
        )
