# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.constants import TaskType
from dbnd._core.task.task import Task


class PythonTask(Task):
    """
    Executes Python code as a dbnd task.

    Example::

        class CalculateAlpha(PythonTask):
            alpha = parameter.value(0.5)
            result = output

            def run(self):
                self.result.write(self.alpha)

    """

    _conf__task_type_name = TaskType.python
