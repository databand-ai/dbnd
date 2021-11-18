import enum

from typing import Optional

import dbnd

from dbnd._core.utils.basics.singleton_context import SingletonContext


class TaskContextPhase(enum.Enum):
    BUILD = 0
    RUN = 1


class TaskContext(SingletonContext):
    def __init__(self, stack, phase):
        super(TaskContext, self).__init__()
        self.stack = stack
        self.phase = phase

    def __repr__(self):
        return "TaskContext( [...%s], %s)" % (self.stack[-1], self.phase)


def task_context(task, phase):
    # type: (dbnd.tasks.Task, TaskContextPhase) -> TaskContext
    base_stack = []

    if has_current_task():
        base_stack = current_task_stack()

    return TaskContext.new_context(
        stack=base_stack + [task], phase=phase, allow_override=True
    )


def has_current_task():
    return TaskContext.has_instance()


def current_task():
    # type: () -> dbnd.tasks.Task
    return TaskContext.get_instance().stack[-1]


def try_get_current_task():
    # type: () -> Optional[dbnd.tasks.Task]
    tc = TaskContext.try_get_instance()
    if tc and tc.stack:
        return tc.stack[-1]
    return None


def current():
    return current_task()


def current_phase():
    # type: () -> TaskContextPhase
    return TaskContext.get_instance().phase


def current_task_stack():
    # type: () -> {}
    return TaskContext.get_instance().stack
