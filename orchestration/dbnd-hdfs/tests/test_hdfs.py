# -*- coding: utf-8 -*-
#
# Copyright 2015 Twitter Inc
# Modifications Copyright (C) 2018 databand.ai
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# This file has been modified by databand.ai to support dbnd orchestration.


"""This is an integration test for the GCS-databand binding.

This test requires credentials that can access GCS & access to a bucket below.
Follow the directions in the gcloud tools to set up local credentials.
"""
import tempfile

import pytest

from dbnd.testing.base_target_test_mixin import FileTargetTestMixin
from targets import target


class _HdfsBaseTestCase(object):
    @pytest.fixture(autouse=True)
    def set_hdfs_client(self):
        from dbnd_hdfs.fs.hdfs_hdfscli import HdfsCli

        self.client = HdfsCli()

    @pytest.fixture(autouse=True)
    def set_hdfs_path(self, hdfs_path):
        self.hdfs_path = hdfs_path

    def folder_path(self, suffix):
        return "{}/test_hdfs/{}".format(self.hdfs_path, suffix)


class TestHdfsClient(_HdfsBaseTestCase):
    def test_not_exists(self):
        assert not self.client.exists(self.folder_path("does_not_exist"))
        assert not self.client.isdir(self.folder_path("does_not_exist"))

    def test_exists(self):
        self.client.put_string("hello", self.folder_path("exists_test"))
        assert self.client.exists(self.folder_path("exists_test"))
        assert not self.client.isdir(self.folder_path("exists_test"))

    def test_mkdir(self):
        self.client.mkdir(self.folder_path("exists_dir_test"))
        assert self.client.exists(self.folder_path("exists_dir_test"))
        assert self.client.isdir(self.folder_path("exists_dir_test"))

    def test_mkdir_by_upload(self):
        self.client.put_string("hello", self.folder_path("test_dir_recursive/yep/file"))
        assert self.client.exists(self.folder_path("test_dir_recursive"))
        assert self.client.isdir(self.folder_path("test_dir_recursive"))

    def test_download(self):
        original_content = "hello"
        self.client.put_string(original_content, self.folder_path("test_download"))
        content = self.client.get_as_string(self.folder_path("test_download"))
        assert original_content == content.decode()

    def test_rename(self):
        self.client.put_string("hello", self.folder_path("test_rename_1"))
        self.client.rename(
            self.folder_path("test_rename_1"), self.folder_path("test_rename_2")
        )
        assert not self.client.exists(self.folder_path("test_rename_1"))
        assert self.client.exists(self.folder_path("test_rename_2"))

    def test_rename_recursive(self):
        self.client.mkdir(self.folder_path("test_rename_recursive"))
        self.client.put_string("hello", self.folder_path("test_rename_recursive/1"))
        self.client.put_string("hello", self.folder_path("test_rename_recursive/2"))
        self.client.rename(
            self.folder_path("test_rename_recursive"),
            self.folder_path("test_rename_recursive_dest"),
        )
        assert not self.client.exists(self.folder_path("test_rename_recursive"))
        assert not self.client.exists(self.folder_path("test_rename_recursive/1"))
        assert self.client.exists(self.folder_path("test_rename_recursive_dest"))
        assert self.client.exists(self.folder_path("test_rename_recursive_dest/1"))

    def test_remove(self):
        self.client.put_string("hello", self.folder_path("test_remove"))
        self.client.remove(self.folder_path("test_remove"))
        assert not self.client.exists(self.folder_path("test_remove"))

    def test_remove_recursive(self):
        self.client.mkdir(self.folder_path("test_remove_recursive"))
        self.client.put_string("hello", self.folder_path("test_remove_recursive/1"))
        self.client.put_string("hello", self.folder_path("test_remove_recursive/2"))
        self.client.remove(self.folder_path("test_remove_recursive"))

        assert not self.client.exists(self.folder_path("test_remove_recursive"))
        assert not self.client.exists(self.folder_path("test_remove_recursive/1"))
        assert not self.client.exists(self.folder_path("test_remove_recursive/2"))

    def test_listdir(self):
        self.client.put_string("hello", self.folder_path("test_listdir/1"))
        self.client.put_string("hello", self.folder_path("test_listdir/2"))

        assert [
            self.folder_path("test_listdir/1"),
            self.folder_path("test_listdir/2"),
        ] == list(self.client.listdir(self.folder_path("test_listdir/")))
        assert [
            self.folder_path("test_listdir/1"),
            self.folder_path("test_listdir/2"),
        ] == list(self.client.listdir(self.folder_path("test_listdir")))

    def test_put_file(self):
        with tempfile.NamedTemporaryFile("w") as fp:
            lorem = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt\n"
            # Larger file than chunk size, fails with incorrect progress set up
            big = lorem * 41943
            fp.write(big)
            fp.flush()

            self.client.put(fp.name, self.folder_path("test_put_file"))
            assert self.client.exists(self.folder_path("test_put_file"))
            assert (
                big
                == self.client.get_as_string(self.folder_path("test_put_file")).decode()
            )


@pytest.mark.hdfs
class TestHdfsTarget(_HdfsBaseTestCase, FileTargetTestMixin):
    def create_target(self, io_pipe=None):
        return target(self.folder_path(self._id), io_pipe=io_pipe, fs=self.client)

    def test_close_twice(self):
        tgt = self.create_target()

        with tgt.open("w") as dst:
            dst.write("data")
        assert dst.closed
        dst.close()
        assert dst.closed

        with tgt.open() as src:
            assert src.read().strip() == "data"
        assert src.closed
        src.close()
        assert src.closed

    @pytest.mark.skip(reason="requires hdfs with kerberos to test")
    def test_kerberos_client(self):
        from dbnd_hdfs.hdfs_config import (
            HdfsConfig,
            KerberosMutualAuthentication,
            WebHdfsClientType,
        )

        conf = HdfsConfig(
            client_type=WebHdfsClientType.KERBEROS,
            kerberos_mutual_authentication=KerberosMutualAuthentication.REQUIRED,
            kerberos_force_preemptive=True,
        )

        client = WebHdfsClient(config=conf)
        trg = target(self.folder_path(self._id), fs=client)
        assert not trg.exists()
