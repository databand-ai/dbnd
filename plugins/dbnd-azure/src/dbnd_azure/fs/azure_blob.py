"""databand bindings for Azure Blob Storage"""

import logging
import os
import time

from azure.storage.blob import BlockBlobService
from six.moves import xrange

from dbnd_azure.fs import AZURE_BLOB_FS_NAME
from targets import AtomicLocalFile
from targets.config import get_local_tempfile
from targets.errors import (
    FileAlreadyExists,
    InvalidDeleteException,
    MissingParentDirectory,
)
from targets.fs.file_system import FileSystem
from targets.utils.atomic import _DeleteOnCloseFile


try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit


logger = logging.getLogger(__name__)
# Time to sleep while waiting for eventual consistency to finish.
EVENTUAL_CONSISTENCY_SLEEP_INTERVAL = 0.1

# Maximum number of sleeps for eventual consistency.
EVENTUAL_CONSISTENCY_MAX_SLEEPS = 300


def _wait_for_consistency(checker):
    """Eventual consistency: wait until Azure reports something is true.

    This is necessary for e.g. create/delete where the operation might return,
    but won't be reflected for a bit.
    """
    for _ in xrange(EVENTUAL_CONSISTENCY_MAX_SLEEPS):
        if checker():
            return

        time.sleep(EVENTUAL_CONSISTENCY_SLEEP_INTERVAL)

    logger.warning(
        "Exceeded wait for eventual Azure consistency - this may be a"
        "bug in the library or something is terribly wrong."
    )


class AzureBlobStorageClient(FileSystem):
    """
    An implementation of a FileSystem over Azure Block Storage.

    """

    name = AZURE_BLOB_FS_NAME

    def __init__(self, **kwargs):
        self._options = kwargs
        self.account = self._options.get("account_name")
        self.conn = self._create_connection()

    def _create_connection(self):
        return BlockBlobService(
            account_name=self._options.get("account_name"),
            account_key=self._options.get("account_key"),
            sas_token=self._options.get("sas_token"),
            protocol=self._options.get("protocol", "https"),
            connection_string=self._options.get("connection_string"),
            endpoint_suffix=self._options.get("endpoint_suffix"),
            custom_domain=self._options.get("custom_domain"),
        )

    @staticmethod
    def _path_to_account_container_and_blob(path):
        (scheme, netloc, path, _, _) = urlsplit(str(path))
        assert scheme == "https"
        path_components = path.split("/")
        return netloc.split(".")[0], path_components[1], "/".join(path_components[2:])

    def _get_path_prefix(self, container=None):
        if container:
            container = self._add_path_delimiter(container)
        return self.conn.protocol + "://" + self.conn.primary_endpoint + "/" + container

    @staticmethod
    def _is_container(blob):
        return not blob

    @staticmethod
    def _add_path_delimiter(key):
        return key if key[-1:] == "/" or key == "" else key + "/"

    def exists(self, path):
        (
            storage_account,
            container_name,
            blob_name,
        ) = self._path_to_account_container_and_blob(path)
        assert self.account == storage_account
        if self._is_container(blob_name):
            return self.conn.exists(container_name)

        if self.isdir(path):
            return True

        return self.conn.exists(container_name, blob_name)

    def isdir(self, path):
        (
            storage_account,
            container_name,
            blob_name,
        ) = self._path_to_account_container_and_blob(path)
        assert self.account == storage_account

        if self._is_container(blob_name):
            containers = [c.name for c in self.conn.list_containers(container_name)]
            return container_name in containers

        matches = self.conn.list_blobs(
            container_name, self._add_path_delimiter(blob_name), num_results=1
        )
        return len(list(matches))

    def remove(self, path, recursive=True, skip_trash=True):
        (
            storage_account,
            container_name,
            prefix,
        ) = self._path_to_account_container_and_blob(path)
        assert self.account == storage_account

        if self._is_container(prefix):
            self.conn.delete_container(container_name)
            _wait_for_consistency(lambda: not self.isdir(path))
            return

        if self.isdir(path):
            if not recursive:
                raise InvalidDeleteException(
                    "Path {} is a directory. Must use recursive delete".format(path)
                )

            blobs = self.listdir(path)
            for b in blobs:
                _, _, blob_to_remove = self._path_to_account_container_and_blob(b)
                self.conn.delete_blob(container_name, blob_to_remove)
            _wait_for_consistency(lambda: not self.isdir(path))
            return

        self.conn.delete_blob(container_name, prefix)
        _wait_for_consistency(lambda: not self.exists(path))
        return

    def mkdir(self, path, parents=True, raise_if_exists=False):
        if raise_if_exists and self.isdir(path):
            raise FileAlreadyExists()

        # storage_account, container_name, prefix = self._path_to_account_container_and_blob(
        #     path
        # )
        # assert self.account == storage_account
        #
        # if self._is_container(prefix):
        #     # isdir raises if the bucket doesn't exist; nothing to do here.
        #     return
        #
        if not parents and not self.isdir(os.path.dirname(path)):
            raise MissingParentDirectory()
        # prefix = self._add_path_delimiter(prefix)
        #
        # self.conn.create_blob_from_bytes(container_name, prefix, b"")

    def copy(self, source_path, destination_path, **kwargs):

        (
            source_account,
            source_container,
            source_blob,
        ) = self._path_to_account_container_and_blob(source_path)
        (
            dest_account,
            dest_container,
            dest_prefix,
        ) = self._path_to_account_container_and_blob(destination_path)
        assert self.account == dest_account

        if self.isdir(source_path):
            blobs = self.conn.list_blobs(
                source_container, self._add_path_delimiter(source_blob)
            )
            copied = []
            for b in blobs:
                blob_suffix = b.name.replace(source_blob, "")
                destination_path = dest_prefix + blob_suffix
                self.conn.copy_blob(
                    dest_container,
                    destination_path,
                    self._get_path_prefix(source_container) + b.name,
                )

                copied.append(destination_path)

            _wait_for_consistency(
                lambda: all(self.conn.exists(dest_container, c) for c in copied)
            )
        else:
            self.conn.copy_blob(dest_container, dest_prefix, source_path)
            _wait_for_consistency(lambda: self.conn.exists(dest_container, dest_prefix))

    def move(self, source_path, destination_path):
        """
        Rename/move an object from one Azure storage location to another.
        """
        self.copy(source_path, destination_path)
        self.remove(source_path)

    def listdir(self, path):
        account, container, blob = self._path_to_account_container_and_blob(path)
        assert self.account == account

        for it in self.conn.list_blobs(container, self._add_path_delimiter(blob)):
            yield self._get_path_prefix(container) + it.name

    def put(self, path, container, blob):
        logging.debug(
            "Uploading file '{path}' to container '{container}' and blob '{blob}'".format(
                path=path, container=container, blob=blob
            )
        )
        self.conn.create_container(container)
        self.conn.create_blob_from_path(container, blob, path)
        _wait_for_consistency(lambda: self.conn.exists(container, blob))

    def put_string(self, content, container, blob):
        logging.debug(
            "Uploading string '{content}' to container '{container}' and blob '{blob}'".format(
                content=content, container=container, blob=blob
            )
        )
        self.conn.create_container(container)
        self.conn.create_blob_from_text(container, blob, content)
        _wait_for_consistency(lambda: self.conn.exists(container, blob))

    def download_as_bytes(self, container, blob, bytes_to_read=None):
        start_range, end_range = (
            (0, bytes_to_read - 1) if bytes_to_read is not None else (None, None)
        )
        logging.debug(
            "Downloading from container '{container}' and blob '{blob}' as bytes".format(
                container=container, blob=blob
            )
        )
        return self.conn.get_blob_to_bytes(
            container, blob, start_range=start_range, end_range=end_range
        ).content

    def download_as_file(self, container, blob, location):
        logging.debug(
            "Downloading from container '{container}' and blob '{blob}' to {location}".format(
                container=container, blob=blob, location=location
            )
        )
        return self.conn.get_blob_to_path(container, blob, location, open_mode="w")

    def mkdir_parent(self, path):
        pass

    def copy_from_local_file(self, local_path, dest, **kwargs):
        account, container_name, blob_name = self._path_to_account_container_and_blob(
            dest
        )

        self.put(path=local_path, container=container_name, blob=blob_name)

    def open_read(self, path, mode="r"):
        account, container, blob = self._path_to_account_container_and_blob(path)
        assert self.account == account

        local_tmp_file = get_local_tempfile("download-%s" % os.path.basename(path))
        # We can't return the tempfile reference because of a bug in python: http://bugs.python.org/issue18879
        self.download_as_file(container, blob, local_tmp_file)

        return _DeleteOnCloseFile(local_tmp_file, mode)

    def download_file(self, azure_path, location, **kwargs):
        account, container, blob = self._path_to_account_container_and_blob(azure_path)
        assert self.account == account

        self.download_as_file(container, blob, location)
        return

    def open_write(self, path, mode="w", **kwargs):
        account, container, blob = self._path_to_account_container_and_blob(path)
        assert self.account == account
        return AtomicAzureBlobFile(container, blob, self, **self._options)


class AtomicAzureBlobFile(AtomicLocalFile):
    def __init__(self, container, blob, client, **kwargs):
        super(AtomicAzureBlobFile, self).__init__(os.path.join(container, blob), client)
        self.container = container
        self.blob = blob
        self.client = client
        self.azure_blob_options = kwargs

    def move_to_final_destination(self):
        self.client.put(self.tmp_path, self.container, self.blob)
