# -*- coding: utf-8 -*-
#
# Copyright 2015 Twitter Inc
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

"""databand bindings for Google Cloud Storage"""
import logging
import mimetypes
import os
import time

import six

from six.moves import xrange

import targets
import targets.errors
import targets.file_target
import targets.fs.file_system
import targets.utils.atomic

from targets.config import get_local_tempfile
from targets.errors import InvalidDeleteException
from targets.fs import FileSystems
from targets.fs.file_system import FileSystem
from targets.utils.atomic import _DeleteOnCloseFile
from targets.utils.path import path_to_bucket_and_key


logger = logging.getLogger(__name__)

try:
    import httplib2

    import google.auth
    from googleapiclient import errors
    from googleapiclient import discovery
    from googleapiclient import http
except ImportError:
    logger.warning(
        "Loading GCS module without the python packages googleapiclient & google-auth. \
        This will crash at runtime if GCS functionality is used."
    )
else:
    # Retry transport and file IO errors.
    RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

# Number of times to retry failed downloads.
NUM_RETRIES = 5

# Number of bytes to send/receive in each request.
CHUNKSIZE = 10 * 1024 * 1024

# Mimetype to use if one can't be guessed from the file extension.
DEFAULT_MIMETYPE = "application/octet-stream"

# Time to sleep while waiting for eventual consistency to finish.
EVENTUAL_CONSISTENCY_SLEEP_INTERVAL = 0.1

# Maximum number of sleeps for eventual consistency.
EVENTUAL_CONSISTENCY_MAX_SLEEPS = 300


def _wait_for_consistency(checker):
    """Eventual consistency: wait until GCS reports something is true.

    This is necessary for e.g. create/delete where the operation might return,
    but won't be reflected for a bit.
    """
    for _ in xrange(EVENTUAL_CONSISTENCY_MAX_SLEEPS):
        if checker():
            return

        time.sleep(EVENTUAL_CONSISTENCY_SLEEP_INTERVAL)

    logger.warning(
        "Exceeded wait for eventual GCS consistency - this may be a"
        "bug in the library or something is terribly wrong."
    )


def get_authenticate_kwargs(oauth_credentials=None, http_=None):
    """Returns a dictionary with keyword arguments for use with discovery

    Prioritizes oauth_credentials or a http client provided by the user
    If none provided, falls back to default credentials provided by google's command line
    utilities. If that also fails, tries using httplib2.Http()

    Used by `gcs.GCSClient` and `bigquery.BigQueryClient` to initiate the API Client
    """
    if oauth_credentials:
        authenticate_kwargs = {"credentials": oauth_credentials}
    elif http_:
        authenticate_kwargs = {"http": http_}
    else:
        # neither http_ or credentials provided
        try:
            # try default credentials
            credentials, _ = google.auth.default()
            authenticate_kwargs = {"credentials": credentials}
        except google.auth.exceptions.DefaultCredentialsError:
            # try http using httplib2
            authenticate_kwargs = {"http": httplib2.Http()}

    return authenticate_kwargs


class GCSClient(FileSystem):
    """An implementation of a FileSystem over Google Cloud Storage.

       There are several ways to use this class. By default it will use the app
       default credentials, as described at https://developers.google.com/identity/protocols/application-default-credentials .
       Alternatively, you may pass an google-auth credentials object. e.g. to use a service account::

         credentials = google.auth.jwt.Credentials.from_service_account_info(
             '012345678912-ThisIsARandomServiceAccountEmail@developer.gserviceaccount.com',
             'These are the contents of the p12 file that came with the service account',
             scope='https://www.googleapis.com/auth/devstorage.read_write')
         client = GCSClient(oauth_credentials=credentails)

        The chunksize parameter specifies how much data to transfer when downloading
        or uploading files.

    .. warning::
      By default this class will use "automated service discovery" which will require
      a connection to the web. The google api client downloads a JSON file to "create" the
      library interface on the fly. If you want a more hermetic build, you can pass the
      contents of this file (currently found at https://www.googleapis.com/discovery/v1/apis/storage/v1/rest )
      as the ``descriptor`` argument.
    """

    name = FileSystems.gcs
    _exist_after_write_consistent = False

    def __init__(
        self,
        oauth_credentials=None,
        descriptor="",
        http_=None,
        chunksize=CHUNKSIZE,
        **discovery_build_kwargs
    ):
        self.chunksize = chunksize
        authenticate_kwargs = get_authenticate_kwargs(oauth_credentials, http_)

        build_kwargs = authenticate_kwargs.copy()
        build_kwargs.update(discovery_build_kwargs)

        if descriptor:
            self.client = discovery.build_from_document(descriptor, **build_kwargs)
        else:
            build_kwargs.setdefault("cache_discovery", False)
            self.client = discovery.build("storage", "v1", **build_kwargs)

    def _path_to_bucket_and_key(self, path):
        return path_to_bucket_and_key(path)

    def _is_root(self, key):
        return len(key) == 0 or key == "/"

    def _add_path_delimiter(self, key):
        return key if key[-1:] == "/" else key + "/"

    def _obj_exists(self, bucket, obj):
        try:
            self.client.objects().get(bucket=bucket, object=obj).execute()
        except errors.HttpError as ex:
            if ex.resp["status"] == "404":
                return False
            raise
        else:
            return True

    def _list_iter(self, bucket, prefix):
        request = self.client.objects().list(bucket=bucket, prefix=prefix)
        response = request.execute()

        while response is not None:
            for it in response.get("items", []):
                yield it

            request = self.client.objects().list_next(request, response)
            if request is None:
                break

            response = request.execute()

    def _do_put(self, media, dest_path):
        bucket, obj = self._path_to_bucket_and_key(dest_path)

        request = self.client.objects().insert(
            bucket=bucket, name=obj, media_body=media
        )
        if not media.resumable():
            return request.execute()

        response = None
        attempts = 0
        while response is None:
            error = None
            try:
                status, response = request.next_chunk()
                if status:
                    logger.debug("Upload progress: %.2f%%", 100 * status.progress())
            except errors.HttpError as err:
                error = err
                if err.resp.status < 500:
                    raise
                logger.warning("Caught error while uploading", exc_info=True)
            except RETRYABLE_ERRORS as err:
                logger.warning("Caught error while uploading", exc_info=True)
                error = err

            if error:
                attempts += 1
                if attempts >= NUM_RETRIES:
                    raise error
            else:
                attempts = 0

        _wait_for_consistency(lambda: self._obj_exists(bucket, obj))
        return response

    def exists(self, path):
        bucket, obj = self._path_to_bucket_and_key(path)
        if self._obj_exists(bucket, obj):
            return True

        return self.isdir(path)

    def isdir(self, path):
        bucket, obj = self._path_to_bucket_and_key(path)
        if self._is_root(obj):
            try:
                self.client.buckets().get(bucket=bucket).execute()
            except errors.HttpError as ex:
                if ex.resp["status"] == "404":
                    return False
                raise

        obj = self._add_path_delimiter(obj)
        if self._obj_exists(bucket, obj):
            return True

        # Any objects with this prefix
        resp = (
            self.client.objects()
            .list(bucket=bucket, prefix=obj, maxResults=20)
            .execute()
        )
        lst = next(iter(resp.get("items", [])), None)
        return bool(lst)

    def remove(self, path, recursive=True):
        (bucket, obj) = self._path_to_bucket_and_key(path)

        if self._is_root(obj):
            raise InvalidDeleteException(
                "Cannot delete root of bucket at path {}".format(path)
            )

        if self._obj_exists(bucket, obj):
            self.client.objects().delete(bucket=bucket, object=obj).execute()
            _wait_for_consistency(lambda: not self._obj_exists(bucket, obj))
            return True

        if self.isdir(path):
            if not recursive:
                raise InvalidDeleteException(
                    "Path {} is a directory. Must use recursive delete".format(path)
                )

            req = http.BatchHttpRequest()
            for it in self._list_iter(bucket, self._add_path_delimiter(obj)):
                req.add(self.client.objects().delete(bucket=bucket, object=it["name"]))
            req.execute()

            _wait_for_consistency(lambda: not self.isdir(path))
            return True

        return False

    def put(self, filename, dest_path, mimetype=None, chunksize=None):
        chunksize = chunksize or self.chunksize
        resumable = os.path.getsize(filename) > 0

        mimetype = mimetype or mimetypes.guess_type(dest_path)[0] or DEFAULT_MIMETYPE
        media = http.MediaFileUpload(
            filename, mimetype=mimetype, chunksize=chunksize, resumable=resumable
        )

        self._do_put(media, dest_path)

        # Implementation B
        # bucket, key = path_to_bucket_and_key(str(dest_path))
        # bucket = self.client.bucket(bucket)
        # blob = bucket.blob(blob_name=key)
        # blob.upload_from_filename(filename=filename,
        #                           content_type=mimetype)

    def _forward_args_to_put(self, kwargs):
        return self.put(**kwargs)

    def put_multiple(
        self, filepaths, remote_directory, mimetype=None, chunksize=None, num_process=1
    ):
        if isinstance(filepaths, six.string_types):
            raise ValueError(
                "filenames must be a list of strings. If you want to put a single file, "
                "use the `put(self, filename, ...)` method"
            )

        put_kwargs_list = [
            {
                "filename": filepath,
                "dest_path": os.path.join(remote_directory, os.path.basename(filepath)),
                "mimetype": mimetype,
                "chunksize": chunksize,
            }
            for filepath in filepaths
        ]

        if num_process > 1:
            from multiprocessing import Pool
            from contextlib import closing

            with closing(Pool(num_process)) as p:
                return p.map(self._forward_args_to_put, put_kwargs_list)
        else:
            for put_kwargs in put_kwargs_list:
                self._forward_args_to_put(put_kwargs)

    def put_string(self, contents, dest_path, mimetype=None):
        mimetype = mimetype or mimetypes.guess_type(dest_path)[0] or DEFAULT_MIMETYPE
        assert isinstance(mimetype, six.string_types)
        if not isinstance(contents, six.binary_type):
            contents = contents.encode("utf-8")
        media = http.MediaIoBaseUpload(
            six.BytesIO(contents), mimetype, resumable=bool(contents)
        )
        self._do_put(media, dest_path)

    def mkdir(self, path, parents=True, raise_if_exists=False):
        if self.exists(path):
            if raise_if_exists:
                raise targets.errors.FileAlreadyExists()
            elif not self.isdir(path):
                raise targets.errors.NotADirectory()
            else:
                return

        self.put_string(b"", self._add_path_delimiter(path), mimetype="text/plain")

    def copy(self, source_path, destination_path, **kwargs):
        src_bucket, src_obj = self._path_to_bucket_and_key(source_path)
        dest_bucket, dest_obj = self._path_to_bucket_and_key(destination_path)

        if self.isdir(source_path):
            src_prefix = self._add_path_delimiter(src_obj)
            dest_prefix = self._add_path_delimiter(dest_obj)

            source_path = self._add_path_delimiter(source_path)
            copied_objs = []
            for obj in self.listdir(source_path):
                suffix = obj[len(source_path) :]

                self.client.objects().copy(
                    sourceBucket=src_bucket,
                    sourceObject=src_prefix + suffix,
                    destinationBucket=dest_bucket,
                    destinationObject=dest_prefix + suffix,
                    body={},
                ).execute()
                copied_objs.append(dest_prefix + suffix)

            _wait_for_consistency(
                lambda: all(self._obj_exists(dest_bucket, obj) for obj in copied_objs)
            )
        else:
            self.client.objects().copy(
                sourceBucket=src_bucket,
                sourceObject=src_obj,
                destinationBucket=dest_bucket,
                destinationObject=dest_obj,
                body={},
            ).execute()
            _wait_for_consistency(lambda: self._obj_exists(dest_bucket, dest_obj))

    def rename(self, *args, **kwargs):
        """
        Alias for ``move()``
        """
        self.move(*args, **kwargs)

    def move(self, source_path, destination_path):
        """
        Rename/move an object from one GCS location to another.
        """
        self.copy(source_path, destination_path)
        self.remove(source_path)

    def listdir(self, path):
        """
        Get an iterable with GCS folder contents.
        Iterable contains paths relative to queried path.
        """
        bucket, obj = self._path_to_bucket_and_key(path)

        obj_prefix = self._add_path_delimiter(obj)
        if self._is_root(obj_prefix):
            obj_prefix = ""

        obj_prefix_len = len(obj_prefix)
        for it in self._list_iter(bucket, obj_prefix):
            yield self._add_path_delimiter(path) + it["name"][obj_prefix_len:]

    def list_wildcard(self, wildcard_path):
        """Yields full object URIs matching the given wildcard.

        Currently only the '*' wildcard after the last path delimiter is supported.

        (If we need "full" wildcard functionality we should bring in gsutil dependency with its
        https://github.com/GoogleCloudPlatform/gsutil/blob/master/gslib/wildcard_iterator.py...)
        """
        path, wildcard_obj = wildcard_path.rsplit("/", 1)
        assert (
            "*" not in path
        ), "The '*' wildcard character is only supported after the last '/'"
        wildcard_parts = wildcard_obj.split("*")
        assert len(wildcard_parts) == 2, "Only one '*' wildcard is supported"

        for it in self.listdir(path):
            if (
                it.startswith(path + "/" + wildcard_parts[0])
                and it.endswith(wildcard_parts[1])
                and len(it)
                >= len(path + "/" + wildcard_parts[0]) + len(wildcard_parts[1])
            ):
                yield it

    def _open_read(
        self,
        remote_path,
        local_path=None,
        delete_file_on_close=True,
        chunksize=None,
        chunk_callback=lambda _: False,
    ):
        """Downloads the object contents to local file system.

        Optionally stops after the first chunk for which chunk_callback returns True.
        """
        chunksize = chunksize or self.chunksize
        bucket, obj = self._path_to_bucket_and_key(remote_path)

        tmp_file_path = local_path or get_local_tempfile(os.path.basename(remote_path))
        with open(tmp_file_path, "wb") as fp:
            # We can't return the tempfile reference because of a bug in python: http://bugs.python.org/issue18879
            if delete_file_on_close:
                return_fp = _DeleteOnCloseFile(tmp_file_path, "r")
            else:
                return_fp = fp

            # Special case empty files because chunk-based downloading doesn't work.
            result = self.client.objects().get(bucket=bucket, object=obj).execute()
            if int(result["size"]) == 0:
                return return_fp

            request = self.client.objects().get_media(bucket=bucket, object=obj)
            downloader = http.MediaIoBaseDownload(fp, request, chunksize=chunksize)

            attempts = 0
            done = False
            while not done:
                error = None
                try:
                    _, done = downloader.next_chunk()
                    if chunk_callback(fp):
                        done = True
                except errors.HttpError as err:
                    error = err
                    if err.resp.status < 500:
                        raise
                    logger.warning("Error downloading file, retrying", exc_info=True)
                except RETRYABLE_ERRORS as err:
                    logger.warning("Error downloading file, retrying", exc_info=True)
                    error = err

                if error:
                    attempts += 1
                    if attempts >= NUM_RETRIES:
                        raise error
                else:
                    attempts = 0

        return return_fp

    def download_file(self, path, location=None, **kwargs):
        """
        Download file to local filesystem
        :param path: remote path
        :param location: local path or none
        :return:
        """
        return self._open_read(
            remote_path=path, local_path=location, delete_file_on_close=False
        )

    def mkdir_parent(self, path):
        pass

    def copy_from_local_file(self, local_path, dest, **kwargs):
        self.put(local_path, dest)

    def open_read(self, path, mode="r"):
        return self._open_read(path)
