import typing


if typing.TYPE_CHECKING:
    from dbnd._core.task.task import Task


class TaskInstanceCache(object):
    def __init__(self):
        self.task_instances = {}
        self.task_obj_instances = {}

    # this is global values for now - we need to understand if it's really required
    # but if task has been created - it's something global, regardless current databand run
    def get_task_obj_by_id(self, task_obj_id):
        # type: (str) -> Task
        return self.task_obj_instances.get(task_obj_id, None)

    def get_task_by_id(self, task_id):
        # type: (str) -> Task
        return self.task_instances.get(task_id, None)

    def register_task_object(self, task):
        # type: (Task) -> Task
        self.task_instances[task.task_id] = task
        return task
