def _task_name(task):
    return task.task_definition.full_task_family


def _band_call_str(task):
    if not task:
        return
    if task._conf__decorator_spec:
        return "%s()" % _task_name(task)
    return "%s.band()" % _task_name(task)


def _run_name(task):
    return task.task_definition.run_name()


def _safe_task_family(task):
    if not task:
        return "unknown"

    from dbnd._core.task import Task

    if isinstance(task, Task):
        return _task_name(task)
    return str(task)


def _parameter_name(task, parameter):
    return "%s.%s" % (task.friendly_task_name, parameter.name)


def _safe_target(target):
    msg = str(target)
    if target and target.source_task:
        msg += " created by {task}".format(task=_safe_task_family(target.source_task))
    return msg
