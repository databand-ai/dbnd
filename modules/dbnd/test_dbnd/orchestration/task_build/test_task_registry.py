# © Copyright Databand.ai, an IBM Company 2022

import logging

import pytest

from dbnd import Task, config, dbnd_cmd, task
from dbnd._core.task_build.task_registry import DbndTaskRegistry, get_task_registry


logger = logging.getLogger(__name__)


class RAmbiguousClass(Task):
    pass


class RAmbiguousClass(Task):  # NOQA
    pass


class TestDbndTaskRegistry(object):
    def test_ambigious(self):
        actual = get_task_registry()._get_task_cls("RAmbiguousClass")
        assert actual == DbndTaskRegistry.AMBIGUOUS_CLASS

    def test_full_name_not_ambigious(self):
        actual = get_task_registry().get_task_cls(
            "test_dbnd.orchestration.task_build.test_task_registry.RAmbiguousClass"
        )
        assert actual == RAmbiguousClass

    def test_no_error_on_same_from(self):
        @task
        def task_with_from():
            return

        with config({"task_with_from": {"_from": "task_with_from"}}):
            get_task_registry().build_dbnd_task("task_with_from")

    def test_error_on_same_from(self):
        with pytest.raises(Exception):
            with config(
                {"unknown_task_with_from": {"_from": "unknown_task_with_from"}}
            ):
                get_task_registry().build_dbnd_task("unknown_task_with_from")


class TestRunDbndTaskRegistry(object):
    def test_shortened_task_name_reference(self, capfd):
        dbnd_cmd(
            "run",
            "dbnd_test_scenarios.test_common.complex_package_structure.complex_package.complex_structure_pipeline",
        )
        output = capfd.readouterr()
        assert "Auto-decorating and treating it as @task" not in output.out
        assert "Auto-decorating and treating it as @task" not in output.err
