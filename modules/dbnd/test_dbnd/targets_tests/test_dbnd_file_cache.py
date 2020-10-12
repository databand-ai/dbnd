import json
import os

from datetime import datetime
from tempfile import NamedTemporaryFile

import mock
import pytest

from mock import MagicMock

from dbnd._vendor.backports_tempfile.tempfile import TemporaryDirectory
from targets import DbndLocalFileMetadataRegistry, target


class TestDbndFileCache(object):
    def test_extensions(self):
        assert DbndLocalFileMetadataRegistry.ext == ".dbnd-meta"

    @pytest.mark.parametrize(
        "raw,expected",
        [
            (None, datetime.now()),
            ("2020-09-05 15:00:00", datetime(2020, 9, 5, 15, 0, 0)),
            (datetime(2020, 9, 5, 15, 0, 0), datetime(2020, 9, 5, 15, 0, 0)),
        ],
    )
    def test_resolve_created_at(self, raw, expected):
        if raw is None:
            assert DbndLocalFileMetadataRegistry._resolve_created_at(raw) >= expected
        else:
            assert DbndLocalFileMetadataRegistry._resolve_created_at(raw) == expected

    @pytest.mark.parametrize(
        "raw,valid",
        [
            ("1h", True),
            ("132h", True),
            ("2d", True),
            ("23d", True),
            ("2s", False),
            ("day", False),
        ],
    )
    def test_is_valid_ttl(self, raw, valid):
        assert DbndLocalFileMetadataRegistry.is_valid_ttl(raw) == valid

    @mock.patch("targets.caching.try_get_databand_run")
    def test_resolve_cache_file_name(self, mock_get_run):
        file_path = "/path/to/file.py"
        with TemporaryDirectory() as dir:
            mock_get_run.return_value.get_current_dbnd_local_root.return_value = target(
                dir
            )
            cache_file_path = DbndLocalFileMetadataRegistry._resolve_cache_file_name(
                file_path
            )
            assert cache_file_path == "{}/cache/file.py{}".format(
                dir, DbndLocalFileMetadataRegistry.ext
            )
            assert os.path.isdir("{}/cache".format(dir)) == True

    @mock.patch("targets.DbndLocalFileMetadataRegistry._resolve_cache_file_name")
    def test_read(self, mock_resolve_file_name):
        content = {
            "file_path": "aaaa.py",
            "local_md5": "_local_md5",
            "remote_md5": "_remote_md5",
            "created_at": str("2020-09-08 15:00:00"),
            "ttl": "1d",
        }

        target = MagicMock()
        with NamedTemporaryFile("w+") as f:
            json.dump(content, f)
            f.flush()

            mock_resolve_file_name.return_value = f.name
            cache = DbndLocalFileMetadataRegistry.read(target)

        assert cache._file_path == content["file_path"]
        assert cache._local_md5 == content["local_md5"]
        assert cache._remote_md5 == content["remote_md5"]
        assert cache._created_at == datetime(2020, 9, 8, 15, 0, 0)
        assert cache._ttl == content["ttl"]
        assert cache._cache_file_path == f.name

    @mock.patch("targets.DbndLocalFileMetadataRegistry._resolve_cache_file_name")
    def test_save(self, mock_resolve_file_name):
        cache = DbndLocalFileMetadataRegistry(
            file_path="aaaa.py",
            local_md5="_local_md5",
            remote_md5="_remote_md5",
            created_at="2020-09-08 15:00:00",
            ttl="1d",
        )

        target = MagicMock()
        with NamedTemporaryFile("w+") as f:
            cache._cache_file_path = f.name
            cache.save()

            mock_resolve_file_name.return_value = f.name
            read_cache = DbndLocalFileMetadataRegistry.read(target)

        assert read_cache == cache

    @mock.patch("targets.DbndLocalFileMetadataRegistry._resolve_cache_file_name")
    def test_expired(self, _):
        cache = DbndLocalFileMetadataRegistry(
            file_path="aaaa.py",
            local_md5="_local_md5",
            remote_md5="_remote_md5",
            created_at="2020-09-08 15:00:00",
            ttl="1d",
        )
        assert cache.expired is True

        cache._created_at = datetime.now()
        assert cache.expired is False
