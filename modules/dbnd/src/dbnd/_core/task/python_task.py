from dbnd._core.constants import TaskType
from dbnd._core.task.data_task import Task


class PythonTask(Task):
    """
    Executes a Python code

    """

    _conf__task_type_name = TaskType.python
