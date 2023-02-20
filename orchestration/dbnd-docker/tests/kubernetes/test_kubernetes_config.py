# © Copyright Databand.ai, an IBM Company 2022

import logging

import pytest

from dbnd import config, extend, new_dbnd_context, override, task
from dbnd._core.configuration.config_readers import read_from_config_stream
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.settings import CoreConfig
from dbnd._core.task_build.task_context import TaskContextPhase
from dbnd._core.task_build.task_registry import build_task_from_config
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.utils import seven


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


@task(
    task_config=dict(kubernetes=dict(labels=extend({"task_name": "the one and only"})))
)
def task_with_extend(name, expected_labels):
    labels = build_task_from_config(task_name=name).labels
    assert all(
        label_name in labels and labels[label_name] == label_value
        for label_name, label_value in expected_labels.items()
    )
    assert "task_name" in labels and labels["task_name"] == "the one and only"


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

    @pytest.mark.parametrize(
        "name, test_config, expected",
        [
            (
                "first_engine",
                """
         [first_engine]
         _type = kubernetes
         labels = {"env_name":"first","special":"only_here"}
         container_repository = ""
         """,
                {"env_name": "first", "special": "only_here"},
            ),
            (
                "second_engine",
                """
             [second_engine]
             _type = kubernetes
             labels = {"env_name": "second"}
             container_repository = ""
             """,
                {"env_name": "second"},
            ),
        ],
    )
    def test_extend_k8s_labels(self, name, test_config, expected):
        with new_dbnd_context(conf={CoreConfig.tracker: "console"}):
            ts = read_from_config_stream(seven.StringIO(test_config))
            with config(ts):
                task_with_extend.dbnd_run(name, expected)
