import typing

from dbnd._core.task_build.task_signature import Signature


if typing.TYPE_CHECKING:
    from dbnd._core.task.task import Task


class TaskInstanceCache(object):
    def __init__(self):
        self.task_instances = {}
        self.task_obj_cache = {}

    # this is global values for now - we need to understand if it's really required
    # but if task has been created - it's something global, regardless current databand run
    def get_cached_task_obj(self, task_obj_cache_signature):
        # type: (Signature) -> Task
        return self.task_obj_cache.get(task_obj_cache_signature.signature, None)

    def get_task_by_id(self, task_id):
        # type: (str) -> Task
        return self.task_instances.get(task_id, None)

    def register_task_instance(self, task):
        # type: (Task) -> None
        self.task_instances[task.task_id] = task

    def register_task_obj_cache_instance(self, task, task_obj_cache_signature):
        self.task_obj_cache[task_obj_cache_signature.signature] = task
