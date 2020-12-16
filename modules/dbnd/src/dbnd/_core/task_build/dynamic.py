from dbnd._core.decorator.task_decorator_spec import build_task_decorator_spec
from dbnd._core.errors import TaskClassNotFoundException
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd._core.task_build.task_registry import get_task_registry


def build_dynamic_task_class(
    task_class, task_family, user_params=None, track_source_code=False
):
    _classdict = _build_classdict(
        task_class, task_family, user_params, track_source_code
    )
    return TaskMetaclass(task_family, (task_class,), _classdict)


def _build_classdict(task_class, task_family, user_params, track_source_code):
    """
    build a classdict as needed in the creation of TaskMetaclass
    """

    attributes = dict(
        _conf__task_family=task_family,
        _conf__decorator_spec=build_task_decorator_spec(
            class_or_func=task_class,
            decorator_kwargs={},
            default_result=parameter.output.pickle[object],
        ),
        __doc__=task_class.__doc__,
        __module__=task_class.__module__,
        _conf__track_source_code=track_source_code,
    )
    if user_params:
        attributes.update(user_params)

    return attributes
