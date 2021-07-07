from threading import RLock

from dbnd._core.utils.seven import get_ident
from dbnd._vendor.cachetools import cached as cachetools_cached, keys as cachetools_keys
from dbnd._vendor.cachetools.keys import _HashedTuple, _kwmark


class memoized_property(object):
    """A read-only @property that is only evaluated once."""

    def __init__(self, fget, doc=None):
        self.fget = fget
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__

    def __get__(self, obj, cls):
        if obj is None:
            return self
        obj.__dict__[self.__name__] = result = self.fget(obj)
        return result


def cached(cache=None, key=cachetools_keys.hashkey, lock=None):
    if cache is None:
        cache = {}
    return cachetools_cached(cache=cache, key=key, lock=lock)


def _thread_safe_hashkey(*args, **kwargs):
    """Return a cache key for the specified hashable arguments."""
    thread_id = get_ident()
    if kwargs:
        return _HashedTuple(args + (thread_id,) + sum(sorted(kwargs.items()), _kwmark))
    else:
        return _HashedTuple(args + (thread_id,))


_per_thread_safe_lock = RLock()


def per_thread_cached(cache=None):
    if cache is None:
        cache = {}
    return cachetools_cached(
        cache=cache, key=_thread_safe_hashkey, lock=_per_thread_safe_lock
    )
