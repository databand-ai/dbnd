import six


if six.PY3:
    import contextlib
else:
    import contextlib2 as contextlib


def qualname_func(func):
    if six.PY3:
        return func.__qualname__
    return "%s.%s" % (func.__module__, func.__name__)


__all__ = ["contextlib", "qualname_func"]
