import abc

from dbnd._core.constants import TaskType
from dbnd._core.task.data_task import Task


class PipelineTask(Task):
    """
    Use for tasks that only wrap other tasks and that by definition are done if all their requirements exist.
    """

    _conf__task_type_name = TaskType.pipeline

    @abc.abstractmethod
    def band(self):
        """
            This is the method you should override while using PipelineTask.
            we need to implement result of this function to be "output" of the task (task_output, and vars)
            Your Pipeline.band() call should have one or more tasks wired one into another.
            See examples!"
        :return:
        """
        return

    def _complete(self):
        if self.task_band and not self.task_band.exists():
            return False
        return super(PipelineTask, self)._complete()


PipelineTask.task_definition.hidden = True
