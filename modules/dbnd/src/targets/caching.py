from typing import Any, Dict

import attr

from targets.config import is_in_memory_cache_target_value


@attr.s(frozen=True)
class TargetCacheKey(object):
    target = attr.ib(converter=str)
    value_type = attr.ib(converter=str)


class TargetCache(object):
    def __init__(self, cache=None):
        self._cache = cache if cache is not None else {}

    def get_cache_group(self):
        # type: () -> Dict[TargetCacheKey, Any]
        return self._cache

    def set(self, value, key):
        if not self.enabled:
            return

        cache = self.get_cache_group()
        if cache is not None:
            cache[key] = value

    def get(self, key):
        if not self.enabled:
            return

        cache = self.get_cache_group()
        if cache is not None:
            return cache.get(key)

    def has(self, key):
        cache = self.get_cache_group()
        if cache is not None:
            return key in cache
        return False

    def __getitem__(self, item):
        return self.get(key=item)

    def __setitem__(self, key, value):
        return self.set(value=value, key=key)

    def __contains__(self, item):
        return self.has(key=item)

    def clear_for_targets(self, targets_to_clear):
        if not targets_to_clear:
            return

        cache = self.get_cache_group()
        if not cache:
            return

        targets_to_clear = set(targets_to_clear)
        for key in list(cache.keys()):
            if key.target in targets_to_clear and key in cache:
                del cache[key]

    def clear(self):
        cache = self.get_cache_group()
        if cache:
            cache.clear()

    def clear_all(self):
        self._cache.clear()

    @property
    def enabled(self):
        return is_in_memory_cache_target_value()


TARGET_CACHE = TargetCache()
