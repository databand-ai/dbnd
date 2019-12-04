import logging

from dbnd import Task
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
            "test_dbnd.task_build.test_task_registry.RAmbiguousClass"
        )
        assert actual == RAmbiguousClass
