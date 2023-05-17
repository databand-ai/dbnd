# © Copyright Databand.ai, an IBM Company 2022

import contextlib

from collections.abc import Callable

from dbnd._vendor import cloudpickle


def qualname_func(func):
    return func.__qualname__


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

try:
    import urlparse
except ImportError:
    from urllib.parse import urlparse

__all__ = [
    "contextlib",
    "cloudpickle",
    "qualname_func",
    "import_errors",
    "Callable",
    "get_ident",
    "urlparse",
    "StringIO",
]
