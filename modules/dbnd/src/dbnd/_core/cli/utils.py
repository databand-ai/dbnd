import functools
import json
import logging
import os

from functools import wraps

from dbnd._vendor.pygtrie import CharTrie


class FastSingletonContext(object):
    _instance = None

    @classmethod
    def try_instance(cls, *args, **kwargs):
        """ Singleton get or create"""
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        return cls._instance

    @classmethod
    def renew_instance(cls, *args, **kwargs):
        cls._instance = None
        return cls.try_instance(*args, **kwargs)


class PrefixStore(object):
    def __init__(self, path):
        self._path = path
        self._indices = dict()
        self._objs = dict()

    def set(self, obj_id, obj):
        self._objs[obj_id] = obj

    def index(self, index_name, obj_ref, obj_id):
        if index_name not in self._indices:
            self._indices[index_name] = CharTrie()

        self._indices[index_name][obj_ref] = obj_id

    def search(self, indices_names, prefix):
        for index_name in indices_names:
            if index_name not in self._indices:
                continue

            index = self._indices[index_name]
            if prefix not in index and not index.has_subtrie(prefix):
                continue

            entries = index.items(prefix)
            if len(entries) == 0:
                continue

            return {obj_ref: self._objs[obj_id] for obj_ref, obj_id in entries}
        return dict()

    def load(self):
        cache = json.load(open(self._path, "r"))

        # see https://github.com/google/pygtrie/issues/9
        indices = dict()
        for index_name, index_val in cache["indices"].items():
            indices[index_name] = CharTrie()
            indices[index_name]._root.__setstate__(index_val)

        self._indices = indices
        self._objs = cache["objs"]

    def save(self):
        cache = dict()

        cache["indices"] = dict()

        # see https://github.com/google/pygtrie/issues/9
        for index_name, index_val in self._indices.items():
            cache["indices"][index_name] = index_val._root.__getstate__()

        cache["objs"] = self._objs

        try:
            cache_dir = os.path.dirname(self._path)
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir)
            json.dump(cache, open(self._path, "w"))
        except Exception as ex:
            logging.error(
                "Failed to save autocompletion store at %s: %s", self._path, ex
            )


def no_errors(ret=None):
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                return ret

        return wrapped

    return decorator


def with_fast_dbnd_context(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        from dbnd import new_dbnd_context, config
        from dbnd._core.settings import CoreConfig

        with config({CoreConfig.tracker: ""}, source="fast_dbnd_context"):
            with new_dbnd_context(name="fast_dbnd_context"):
                f(*args, **kwargs)

    return wrapper
