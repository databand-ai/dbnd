from dbnd._core.task_build.task_const import _SAME_AS_PYTHON_MODULE
from dbnd._core.task_build.task_registry import get_task_registry


def namespace(namespace=None, scope=""):
    """
    Call to set namespace of tasks declared after the call.

    It is often desired to call this function with the keyword argument
    ``scope=__name__``.

    The ``scope`` keyword makes it so that this call is only effective for task
    classes with a matching ``__module__``. The default value for
    ``scope`` is the empty string, which means all classes. Multiple calls with
    the same scope simply replace each other.

    The namespace of a ``Task`` can also be changed by specifying the property
    ``task_namespace``.

    ::

        class Task2(dbnd.Task):
            task_namespace = 'namespace2'

    This explicit setting takes priority over whatever is set in the
    ``namespace()`` method, and it's also inherited through normal python
    inheritence.

    There's no equivalent way to set the ``task_family``.
    """
    get_task_registry().register_namespace(scope=scope, namespace=namespace or "")


def auto_namespace(scope=""):
    """
    Same as ``namespace``, but it will be set to the ``__module__`` of the task class.

    This is desirable for these reasons:

    *   Two tasks with the same name will not have conflicting task families
    *   It's more pythonic, as modules are Python's recommended way to do namespacing.
    *   It's traceable. When you see the full name of a task, you can
        immediately identify where it is defined.

    We recommend calling this function from your package's outermost
    ``__init__.py`` file. The file contents could look like this::

        import dbnd

        databand.auto_namespace(scope=__name__)

    To reset an ``auto_namespace()`` call, you can use ``namespace(scope='my_scope')``.
    But this will not be needed (and is also discouraged) if you use the ``scope`` kwarg.
    """
    namespace(namespace=_SAME_AS_PYTHON_MODULE, scope=scope)
