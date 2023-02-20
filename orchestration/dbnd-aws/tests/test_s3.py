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
import sys
import tempfile

import pytest

from dbnd.testing.base_target_test_mixin import FileTargetTestMixin
from dbnd_aws.fs import s3
from targets import target


class _S3BaseTestCase(object):
    @pytest.fixture(autouse=True)
    def set_s3_client(self):
        self.client = s3.S3Client()

    @pytest.fixture(autouse=True)
    def set_s3_path(self, s3_path):
        self.s3_path = s3_path

    def bucket_url(self, suffix):
        """
        Actually it's bucket + test folder name
        """
        return "{}/test_s3/{}".format(self.s3_path, suffix)


@pytest.mark.aws
class TestS3Client(_S3BaseTestCase):
    def test_not_exists(self):
        assert not self.client.exists(self.bucket_url("does_not_exist"))
        assert not self.client.isdir(self.bucket_url("does_not_exist"))

    def test_exists(self):
        self.client.put_string("hello", self.bucket_url("exists_test"))
        assert self.client.exists(self.bucket_url("exists_test"))
        assert not self.client.isdir(self.bucket_url("exists_test"))

    def test_mkdir(self):
        self.client.mkdir(self.bucket_url("exists_dir_test"))
        assert self.client.exists(self.bucket_url("exists_dir_test"))
        assert self.client.isdir(self.bucket_url("exists_dir_test"))

    def test_mkdir_by_upload(self):
        self.client.put_string("hello", self.bucket_url("test_dir_recursive/yep/file"))
        assert self.client.exists(self.bucket_url("test_dir_recursive"))
        assert self.client.isdir(self.bucket_url("test_dir_recursive"))

    def test_download(self):
        self.client.put_string("hello", self.bucket_url("test_download"))
        content = self.client.get_as_string(self.bucket_url("test_download"))
        assert "hello" == content

    def test_rename(self):
        self.client.put_string("hello", self.bucket_url("test_rename_1"))
        self.client.rename(
            self.bucket_url("test_rename_1"), self.bucket_url("test_rename_2")
        )
        assert not self.client.exists(self.bucket_url("test_rename_1"))
        assert self.client.exists(self.bucket_url("test_rename_2"))

    def test_rename_recursive(self):
        self.client.mkdir(self.bucket_url("test_rename_recursive"))
        self.client.put_string("hello", self.bucket_url("test_rename_recursive/1"))
        self.client.put_string("hello", self.bucket_url("test_rename_recursive/2"))
        self.client.rename(
            self.bucket_url("test_rename_recursive"),
            self.bucket_url("test_rename_recursive_dest"),
        )
        assert not self.client.exists(self.bucket_url("test_rename_recursive"))
        assert not self.client.exists(self.bucket_url("test_rename_recursive/1"))
        assert self.client.exists(self.bucket_url("test_rename_recursive_dest"))
        assert self.client.exists(self.bucket_url("test_rename_recursive_dest/1"))

    def test_remove(self):
        self.client.put_string("hello", self.bucket_url("test_remove"))
        self.client.remove(self.bucket_url("test_remove"))
        assert not self.client.exists(self.bucket_url("test_remove"))

    def test_remove_recursive(self):
        self.client.mkdir(self.bucket_url("test_remove_recursive"))
        self.client.put_string("hello", self.bucket_url("test_remove_recursive/1"))
        self.client.put_string("hello", self.bucket_url("test_remove_recursive/2"))
        self.client.remove(self.bucket_url("test_remove_recursive"))

        assert not self.client.exists(self.bucket_url("test_remove_recursive"))
        assert not self.client.exists(self.bucket_url("test_remove_recursive/1"))
        assert not self.client.exists(self.bucket_url("test_remove_recursive/2"))

    def test_listdir(self):
        self.client.put_string("hello", self.bucket_url("test_listdir/1"))
        self.client.put_string("hello", self.bucket_url("test_listdir/2"))

        assert [
            self.bucket_url("test_listdir/1"),
            self.bucket_url("test_listdir/2"),
        ] == list(self.client.listdir(self.bucket_url("test_listdir/")))
        assert [
            self.bucket_url("test_listdir/1"),
            self.bucket_url("test_listdir/2"),
        ] == list(self.client.listdir(self.bucket_url("test_listdir")))

    def test_put_file(self):
        with tempfile.NamedTemporaryFile("w") as fp:
            lorem = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt\n"
            # Larger file than chunk size, fails with incorrect progress set up
            big = lorem * 41943
            fp.write(big)
            fp.flush()

            self.client.put(fp.name, self.bucket_url("test_put_file"))
            assert self.client.exists(self.bucket_url("test_put_file"))
            assert big == self.client.get_as_string(self.bucket_url("test_put_file"))


@pytest.mark.aws
class TestS3Target(_S3BaseTestCase, FileTargetTestMixin):
    def create_target(self, io_pipe=None):
        return target(self.bucket_url(self._id), io_pipe=io_pipe, fs=self.client)

    @pytest.mark.skipif(sys.version_info <= (3, 6), reason="requires python36")
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
