import json
import tempfile

import pytest

from dbnd import config
from targets import target
from dbnd_azure.fs.azure_blob import AzureBlobStorageClient
from test_dbnd.targets_tests.base_target_test_mixin import FileTargetTestMixin


ATTEMPTED_CONTAINER_CREATE = False


class _AzureBlobBaseTestCase(object):
    def _get_credentials_dict(self):
        with open(self.credentials_filename, "r") as file_obj:
            return json.load(file_obj)

    def container_url(self, suffix=None):
        if suffix:
            return "https://{}.blob.core.windows.net/{}/{}".format(
                self.storage_account, self.container_name, suffix
            )
        return "https://{}.blob.core.windows.net/{}".format(
            self.storage_account, self.container_name
        )

    @pytest.fixture(autouse=True)
    def set_azure_client(self):
        config.load_system_configs()
        self.credentials_filename = config.get("azure_tests", "credentials_file")
        self.storage_account = config.get("azure_tests", "storage_account")
        self.container_name = config.get("azure_tests", "container_name")
        self.client = AzureBlobStorageClient(**self._get_credentials_dict())
        global ATTEMPTED_CONTAINER_CREATE
        if not ATTEMPTED_CONTAINER_CREATE:
            self.client.put_string(b"", self.container_name, "create_marker")
            ATTEMPTED_CONTAINER_CREATE = True
        yield self.client


@pytest.mark.azure
class TestAzureBlobClient(_AzureBlobBaseTestCase):
    def test_parse_path(self):
        account, container, blob = AzureBlobStorageClient._path_to_account_container_and_blob(
            "https://myaccount.blob.core.windows.net/mycontainer/folder1/folder2/myblob"
        )
        assert account == "myaccount"
        assert container == "mycontainer"
        assert blob == "folder1/folder2/myblob"

    def test_not_exists(self):
        assert not self.client.exists(self.container_url("does_not_exist"))
        assert not self.client.isdir(self.container_url("does_not_exist"))

    def test_exists(self):
        self.client.put_string("hello", self.container_name, "exists_test")
        assert self.client.exists(self.container_url("exists_test"))

    # def test_mkdir(self):
    #     self.client.mkdir(container_url("exists_dir_test"))
    #     assert self.client.exists(container_url("exists_dir_test/"))
    #     assert self.client.isdir(container_url("exists_dir_test"))

    def test_mkdir_by_upload(self):
        self.client.put_string(
            "hello", self.container_name, "test_dir_recursive/yep/file"
        )
        assert self.client.exists(self.container_url("test_dir_recursive"))
        assert self.client.isdir(self.container_url("test_dir_recursive"))

    def test_download(self):
        self.client.put_string("hello", self.container_name, "test_download")
        content = self.client.download_as_bytes(self.container_name, "test_download")
        assert b"hello" == content

    def test_rename(self):
        self.client.put_string("hello", self.container_name, "test_rename_1")
        self.client.rename(
            self.container_url("test_rename_1"), self.container_url("test_rename_2")
        )

        assert not self.client.exists(self.container_url("test_rename_1"))
        assert self.client.exists(self.container_url("test_rename_2"))

    def test_rename_recursive(self):
        self.client.mkdir(self.container_url("test_rename_recursive"))
        self.client.put_string("hello", self.container_name, "test_rename_recursive/1")
        self.client.put_string("hello", self.container_name, "test_rename_recursive/2")
        self.client.rename(
            self.container_url("test_rename_recursive"),
            self.container_url("test_rename_recursive_dest"),
        )
        assert not self.client.exists(self.container_url("test_rename_recursive"))
        assert not self.client.exists(self.container_url("test_rename_recursive/1"))
        assert self.client.exists(self.container_url("test_rename_recursive_dest"))
        assert self.client.exists(self.container_url("test_rename_recursive_dest/1"))

    def test_remove(self):
        self.client.put_string("hello", self.container_name, "test_remove")
        self.client.remove(self.container_url("test_remove"))
        assert not self.client.exists(self.container_url("test_remove"))

    def test_remove_recursive(self):
        self.client.mkdir(self.container_url("test_remove_recursive"))
        self.client.put_string("hello", self.container_name, "test_remove_recursive/1")
        self.client.put_string("hello", self.container_name, "test_remove_recursive/2")
        self.client.remove(self.container_url("test_remove_recursive"))

        assert not self.client.exists(self.container_url("test_remove_recursive"))
        assert not self.client.exists(self.container_url("test_remove_recursive/1"))
        assert not self.client.exists(self.container_url("test_remove_recursive/2"))

    def test_listdir(self):
        self.client.put_string("hello", self.container_name, "test_listdir/1")
        self.client.put_string("hello", self.container_name, "test_listdir/2")

        assert [
            self.container_url("test_listdir/1"),
            self.container_url("test_listdir/2"),
        ] == list(self.client.listdir(self.container_url("test_listdir/")))
        # assert [self.container_url("test_listdir/1"), self.container_url("test_listdir/2")] == list(
        #     self.client.listdir(self.container_url("test_listdir"))
        # )

    def test_put_file(self):
        with tempfile.NamedTemporaryFile() as fp:
            lorem = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt\n"
            # Larger file than chunk size, fails with incorrect progress set up
            big = lorem * 41943
            fp.write(big)
            fp.flush()

            self.client.put(fp.name, self.container_name, "test_put_file")
            assert self.client.exists(self.container_url("test_put_file"))
            assert big == self.client.download_as_bytes(
                self.container_name, "test_put_file"
            )


@pytest.mark.azure
class TestAzureBlobTarget(_AzureBlobBaseTestCase, FileTargetTestMixin):
    def create_target(self, io_pipe=None):
        return target(self.container_url(self._id), io_pipe=io_pipe, fs=self.client)

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
