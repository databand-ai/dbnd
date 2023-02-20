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
import base64
import os
import tempfile
import uuid

import google.auth
import google.oauth2.service_account
import pytest

from dbnd import config
from dbnd.testing.base_target_test_mixin import FileTargetTestMixin
from dbnd_gcp.fs import gcs
from targets import target


pytest.importorskip("googleapiclient.errors")
pytest.importorskip("google.auth")


TEST_FOLDER = str(uuid.uuid4())


class _GCSBaseTestCase(object):
    def _get_credentials(self):
        scope = config.get("gcp_tests", "scope")
        return google.auth.default(scopes=[scope])[0]

    @pytest.fixture(autouse=True)
    def set_gcs_client(self):
        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") is None:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                credentials_json = config.get("gcp_tests", "credentials_json")
                f.write(base64.b64decode(credentials_json))
                os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", f.name)
        credentials = self._get_credentials()
        self.client = gcs.GCSClient(credentials)

    def bucket_url(self, suffix):
        """
        Actually it's bucket + test folder name
        """
        bucket_name = config.get("gcp_tests", "bucket_name")
        return "gs://{}/{}/{}".format(bucket_name, TEST_FOLDER, suffix)


@pytest.mark.gcp
class TestGCSClient(_GCSBaseTestCase):
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
        fp = self.client.download(self.bucket_url("test_download"))
        assert b"hello" == fp.read()

    def test_rename(self):
        self.client.put_string("hello", self.bucket_url("test_rename_1"))
        self.client.rename(
            self.bucket_url("test_rename_1"), self.bucket_url("test_rename_2")
        )
        assert not self.client.exists(self.bucket_url("test_rename_1"))
        assert self.client.exists(self.bucket_url("test_rename_2"))

    @pytest.mark.skip("fails on ConnectionResetError")
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
        with tempfile.NamedTemporaryFile() as fp:
            lorem = b"Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt\n"
            # Larger file than chunk size, fails with incorrect progress set up
            big = lorem * 41943
            fp.write(big)
            fp.flush()

            self.client.put(fp.name, self.bucket_url("test_put_file"))
            assert self.client.exists(self.bucket_url("test_put_file"))
            assert big == self.client.download(self.bucket_url("test_put_file")).read()

    # TODO: fix this test
    @pytest.mark.skip
    def test_put_file_multiproc(self):
        temporary_fps = []
        for _ in range(2):
            fp = tempfile.NamedTemporaryFile(mode="wb")

            lorem = b"Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt\n"
            # Larger file than chunk size, fails with incorrect progress set up
            big = lorem * 41943
            fp.write(big)
            fp.flush()
            temporary_fps.append(fp)

        filepaths = [f.name for f in temporary_fps]
        self.client.put_multiple(filepaths, self.bucket_url(""), num_process=2)

        for fp in temporary_fps:
            basename = os.path.basename(fp.name)
            assert self.client.exists(self.bucket_url(basename))
            assert big == self.client.download(self.bucket_url(basename)).read()
            fp.close()


@pytest.mark.gcp
class TestGCSTarget(_GCSBaseTestCase, FileTargetTestMixin):
    def create_target(self, io_pipe=None):
        return target(self.bucket_url(self._id), io_pipe=io_pipe, fs=self.client)

    def assertCleanUp(self, tmp_path=""):
        pass

    def test_close_twice(self):
        # Ensure gcs._DeleteOnCloseFile().close() can be called multiple times
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
