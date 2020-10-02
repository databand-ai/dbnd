import pytest

from dbnd import new_dbnd_context, pipeline, task
from dbnd._core.errors import DatabandBuildError
from dbnd._core.errors.base import DatabandRunError


class TestCommonBuildErrors(object):
    def test_circle_dependency_type(self):
        @task
        def task_a(a):
            return a

        @task
        def task_b(a):
            return a

        @pipeline
        def task_circle():
            a = task_a(1)
            b = task_b(a)
            b.task.set_downstream(a)
            return b

        with new_dbnd_context(conf={"core": {"recheck_circle_dependencies": "True"}}):
            with pytest.raises(
                DatabandBuildError, match="A cyclic dependency occurred"
            ):
                task_circle.task()

        with pytest.raises(DatabandRunError, match="A cyclic dependency occurred"):
            task_circle.dbnd_run()
