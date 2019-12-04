import logging
import sys

from dbnd import task
from dbnd._core.task_ctrl.task_visualiser import _MAX_VALUE_SIZE, TaskVisualiser
from test_dbnd.factories import TTask


logger = logging.getLogger(__name__)


@task
def t_very_long_params(t_param="long_string" * 1000):
    return "ok"


class TestTaskVisualizer(object):
    def test_simple_dump(self):
        s = TTask(t_param="my_param")
        actual = TaskVisualiser(s).banner("Runinng task")
        assert "my_param" in actual

    def test_exception(self):
        s = TTask(t_param="my_param")
        try:
            raise Exception("MyException")
        except Exception:
            actual = TaskVisualiser(s).banner("Runinng task", exc_info=sys.exc_info())
            assert actual
            assert "MyException" in actual

    def test_in_memory_dump(self):
        s = t_very_long_params.task(t_param="long_string" * 1000)
        assert len(s.t_param) > _MAX_VALUE_SIZE * 3

        actual = TaskVisualiser(s).banner("Running task")
        logger.warning(actual)
        assert len(actual) < _MAX_VALUE_SIZE * 3
