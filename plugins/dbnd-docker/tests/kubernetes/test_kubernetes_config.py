import logging

from dbnd import override, task
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.task_build.task_context import TaskContextPhase
from dbnd._core.task_build.task_registry import build_task_from_config
from dbnd._core.task_run.task_run import TaskRun


def dynamically_calculated():
    logging.error("Calculating config value for test! test_f_value")
    return "test_f_value"


@task(
    task_config=dict(
        kubernetes=dict(
            cluster_context=override("test"),
            limits={"test_limits": 1},
            container_tag="@python://test_dbnd.task.task_configuration.test_task_config.dynamically_calculated",
        )
    )
)
def dummy_nested_config_task(expected, config_name):
    # type: ( object, str)-> object

    # explicitly build config for k8s
    actual = build_task_from_config(task_name=config_name)

    return (actual.limits, actual.cluster_context, actual.container_tag)


class TestTaskConfig(object):
    def test_task_config_override(self):
        actual = dummy_nested_config_task.dbnd_run(
            "test_limits", config_name="gcp_k8s_engine"
        )
        assert actual.task.result.load(object) == (
            {"test_limits": 1},
            "test",
            "test_f_value",
        )

    def test_task_runner_context(self):
        actual = dummy_nested_config_task.dbnd_run(
            "test_limits", config_name="gcp_k8s_engine"
        )
        with DatabandRun.context(actual):
            task_run = actual.task.current_task_run  # type: TaskRun

            with task_run.task.ctrl.task_context(phase=TaskContextPhase.BUILD):
                actual = build_task_from_config(task_name="gcp_k8s_engine")
                assert actual.cluster_context == "test"
                assert actual.container_tag == "test_f_value"
