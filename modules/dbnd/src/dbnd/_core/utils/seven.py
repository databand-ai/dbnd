import sys

import six


if six.PY3:
    import contextlib
    from collections.abc import Callable
else:
    import contextlib2 as contextlib
    from collections import Callable


def qualname_func(func):
    if six.PY3:
        return func.__qualname__
    return "%s.%s" % (func.__module__, func.__name__)


try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

try:
    import_errors = (ImportError, ModuleNotFoundError)
except Exception:
    # we are python2
    import_errors = (ImportError,)

try:
    from thread import get_ident
except ImportError:
    try:
        from _thread import get_ident
    except ImportError:
        from _dummy_thread import get_ident


def fix_sys_path_str():
    if six.PY2:
        # fix path from "non" str values, otherwise we fail on py2
        sys.path = [str(p) if type(p) != str else p for p in sys.path]


try:
    import urlparse
except ImportError:
    from urllib.parse import urlparse

__all__ = [
    "contextlib",
    "qualname_func",
    "import_errors",
    "Callable",
    "get_ident",
    "fix_sys_path_str",
    "urlparse",
]
