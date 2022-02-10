import json
import os
import re

from datetime import datetime, timedelta
from typing import Any, Dict

import attr
import six

from dbnd._core.current import try_get_databand_run
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


class DbndLocalFileMetadataRegistry(object):
    """
    :type file_path: str
    :type local_md5: str
    :type remote_md5: str
    :type created_at: datetime.datetime
    :type ttl: str
    """

    ext = ".dbnd-meta"
    _date_format = "%Y-%m-%d %H:%M:%S"
    default_ttl = "1d"

    def __init__(
        self, file_path, local_md5=None, remote_md5=None, created_at=None, ttl=None
    ):
        self._file_path = file_path
        self._cache_file_path = self._resolve_cache_file_name(file_path)
        self._local_md5 = local_md5
        self._remote_md5 = remote_md5
        self._created_at = self._resolve_created_at(created_at)

        self._ttl = ttl or DbndLocalFileMetadataRegistry.default_ttl
        if not self.is_valid_ttl(self._ttl):
            raise Exception(
                "Valid ttl should be inf form of 1-9*d|1-9*h, is %s".format(self._ttl)
            )

    def __eq__(self, other):
        return all(
            [
                self._file_path == other._file_path,
                self._cache_file_path == other._cache_file_path,
                self._local_md5 == other._local_md5,
                self._remote_md5 == other._remote_md5,
                self._created_at == other._created_at,
            ]
        )

    def __repr__(self):
        return "DbndFileCache<{}:{}:{}>".format(
            self._file_path, self._created_at, self._ttl
        )

    @staticmethod
    def _resolve_created_at(created_at):
        if not created_at:
            return datetime.now()
        if six.PY2:
            created_at = str(created_at)
        if isinstance(created_at, str):
            return datetime.strptime(
                created_at, DbndLocalFileMetadataRegistry._date_format
            )
        if isinstance(created_at, datetime):
            return created_at

        raise Exception(
            "`created_at` parameter must be either datetime object or isoformat date string"
        )

    @staticmethod
    def is_valid_ttl(ttl):
        """Check if provided ttl value is in supported format i.e 1h, 12h, 2d"""
        return bool(re.match(r"\d*d$|\d*h$", ttl))

    @staticmethod
    def _resolve_cache_file_name(file_path):
        run = try_get_databand_run()
        if not run:
            raise Exception("No databand run found to when creating cache file")
        dbnd_local_root = run.get_current_dbnd_local_root()
        cache_dir = get_or_create_folder_in_dir("cache", dbnd_local_root.path)

        file_name = os.path.basename(file_path) + DbndLocalFileMetadataRegistry.ext
        return os.path.join(cache_dir, file_name)

    @classmethod
    def read(cls, file_target):
        """Read file cache and create new instance of DbndFileCache"""
        cache_file_path = cls._resolve_cache_file_name(file_target.path)
        with open(cache_file_path, "r+") as f:
            kwargs = json.loads(f.read())
        return cls(**kwargs)

    def save(self):
        """Save DbndFileCache instance to json-like cache file"""
        data = {
            "file_path": self._file_path,
            "local_md5": self._local_md5,
            "remote_md5": self._remote_md5,
            "created_at": self._created_at.strftime(
                DbndLocalFileMetadataRegistry._date_format
            ),
            "ttl": self._ttl,
        }
        with open(self._cache_file_path, "w+") as f:
            f.write(json.dumps(data))

    @staticmethod
    def exists(file_target):
        """Read existing cache file, if it not exists then return None"""
        cache_file_path = DbndLocalFileMetadataRegistry._resolve_cache_file_name(
            file_target.path
        )
        return os.path.exists(cache_file_path)

    def delete(self):
        """Remove cache file"""
        if os.path.isfile(self._cache_file_path):
            os.remove(self._cache_file_path)

    @property
    def expired(self):
        """Check if cache file is still valid"""
        time_map = {"d": timedelta(days=1), "h": timedelta(hours=1)}
        interval_type = self._ttl[-1]
        interval_value = int(self._ttl[:-1])
        expired_at = self._created_at + interval_value * time_map[interval_type]
        return expired_at < datetime.now()

    def validate_local_md5(self, md5_hash):
        return self._local_md5 == md5_hash

    @staticmethod
    def get_or_create(file_target):
        dbnd_meta_cache_exists = DbndLocalFileMetadataRegistry.exists(file_target)
        if dbnd_meta_cache_exists:
            return DbndLocalFileMetadataRegistry.read(file_target)
        else:
            dbnd_meta_cache = DbndLocalFileMetadataRegistry(file_path=file_target.path)
            dbnd_meta_cache.save()
            return dbnd_meta_cache

    @staticmethod
    def refresh(file_target):
        dbnd_meta_cache = DbndLocalFileMetadataRegistry.get_or_create(file_target)
        if dbnd_meta_cache.expired:
            dbnd_meta_cache.delete()
            return DbndLocalFileMetadataRegistry.get_or_create(file_target)


def get_or_create_folder_in_dir(folder_name, dir_):
    # type: (str, Task) -> str
    folder_path = os.path.join(dir_, folder_name)
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)
    return folder_path
