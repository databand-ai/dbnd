# -*- coding: utf-8 -*-
#
# Copyright 2017 https://github.com/alexmilowski/pyox
# Modifications copyright (C) 2018 databand.ai
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

import os
import warnings

from glob import glob
from os.path import isfile

from dbnd import Config, parameter
from targets import FileSystem
from targets.config import get_local_tempfile
from targets.utils.atomic import _DeleteOnCloseFile


HDFS_SCHEMA = "hdfs://"


class HdfsPyox(FileSystem, Config):
    _conf__task_family = "hdfs_knox"

    host = parameter.c(description="HDFS name node URL", default="localhost")[str]
    port = parameter.c(description="HDFS name node port", default=50070)[int]
    user = parameter.c(default="root")[str]
    base = parameter.c(
        default=None,
        description="Base url for Knox, mutually exclusive with HdfsConfig.host",
    )[str]
    secure = parameter.c(default=False)[bool]
    gateway = parameter.c(default=None)[str]
    password = parameter.c(default=None)[str]
    cookies = parameter.c(default=None)[str]
    bearer_token = parameter.c(default=None)[str]
    bearer_token_encode = parameter.c(default=True)[bool]

    @staticmethod
    def _remove_schema(path):
        if path.startswith(HDFS_SCHEMA):
            return path[7:]
        return path

    @property
    def client(self):  # type ()-> WebHDFS
        from pyox import WebHDFS

        return WebHDFS(
            host=self.host,
            port=self.port,
            username=self.user,
            base=self.base,
            secure=self.secure,
            gateway=self.gateway,
            password=self.password,
            cookies=self.cookies,
            bearer_token=self.bearer_token,
            bearer_token_encode=self.bearer_token_encode,
        )

    def walk(self, path, depth=1):
        raise NotImplementedError(
            "move() not implemented on {0}".format(self.__class__.__name__)
        )

    def exists(self, path):
        from pyox.client import ServiceError

        try:
            self.client.status(self._remove_schema(path))
            return True

        except ServiceError as e:
            if e.status_code == 404:
                return False
            else:
                raise e

    def isdir(self, path):
        from pyox.client import ServiceError

        try:
            status = self.client.status(self._remove_schema(path))
            if not status:
                return False
            return status["type"] == "DIRECTORY"
        except ServiceError as e:
            if e.status_code == 404:
                return False
            else:
                raise e

    def put_string(self, content, hdfs_path, **kwargs):
        return self.client.copy(content, self._remove_schema(hdfs_path), **kwargs)

    def mkdir(self, path, parents=True, mode=0o755, raise_if_exists=False):
        """
        Has no returnvalue (just like WebHDFS)
        """
        if not parents or raise_if_exists:
            warnings.warn("webhdfs mkdir: parents/raise_if_exists not implemented")

        permission = int(oct(mode)[2:])  # Convert from int(decimal) to int(octal)
        self.client.make_directory(self._remove_schema(path), permission=permission)

    def download_file(
        self, hdfs_path, local_path, overwrite=False, n_threads=-1, **kwargs
    ):
        if overwrite or n_threads != -1:
            warnings.warn("webhdfs download: overwrite/n_threads not implemented")

        input_fd = self.client.open(self._remove_schema(hdfs_path))
        with open(local_path, "wb") as output_fd:
            for chunk in input_fd:
                output_fd.write(chunk)

        return local_path

    def get_as_string(self, hdfs_path):
        input_fd = self.client.open(self._remove_schema(hdfs_path))
        content_as_list = []
        for chunk in input_fd:
            content_as_list.append(chunk)

        return b"".join(content_as_list)

    def move(self, path, dest):
        path = self._remove_schema(path)
        dest = self._remove_schema(dest)
        self.client.move(path, dest)

    def remove(self, hdfs_path, recursive=True, skip_trash=True):
        return self.client.remove(self._remove_schema(hdfs_path), recursive=recursive)

    def listdir(
        self,
        path,
        ignore_directories=False,
        ignore_files=False,
        include_size=False,
        include_type=False,
        include_time=False,
        recursive=False,
    ):
        assert not recursive
        for obj in self.client.list_directory(self._remove_schema(path)):
            yield os.path.join(path, obj)

    def upload(
        self, hdfs_path, local_path, overwrite=False, force=False, recursive=False
    ):
        destpath = self._remove_schema(hdfs_path)

        if destpath[-1] == "/":
            if isfile(local_path):
                self._copy_to_destination(local_path, destpath, force=force)
            else:
                files = glob(local_path, recursive=recursive)
                mkdir = set()
                for source in files:
                    self._copy_to_destination(source, destpath, mkdir, force=force)

        else:
            source = local_path
            size = -1
            with open(source, "rb") as input:
                if not self.client.copy(
                    input, destpath, size=size, overwrite=overwrite
                ):
                    from pyox.client import ServiceError

                    raise ServiceError(
                        403, "Move failed: {} → {}".format(source, destpath)
                    )
        return destpath

    def _copy_to_destination(self, source, destpath, mkdirs=None, force=False):
        if mkdirs is None:
            mkdirs = set()
        size = os.path.getsize(source)
        targetpath = source
        slash = source.rfind("/")
        if source[0] == "/":
            targetpath = source[slash + 1 :]
        elif source[0:3] == "../":
            targetpath = source[slash + 1 :]
        elif slash > 0:
            dirpath = source[0:slash]
            if dirpath not in mkdirs:
                if self.client.make_directory(destpath + dirpath):
                    mkdirs.add(dirpath)
                else:
                    from pyox.client import ServiceError

                    raise ServiceError(
                        403, "Cannot make target directory: {}".format(dirpath)
                    )

        target = destpath + targetpath

        with open(source, "rb") as input:

            def chunker():
                sent = 0
                while True:
                    b = input.read(32768)
                    sent += len(b)
                    yield b

            if not self.client.copy(
                chunker() if size < 0 else input, target, size=size, overwrite=force
            ):
                from pyox.client import ServiceError

                raise ServiceError(403, "Move failed: {} → {}".format(source, target))
        return target

    def open_read(self, path, mode="r"):
        local_tmp_file = get_local_tempfile("download-%s" % os.path.basename(path))
        # We can't return the tempfile reference because of a bug in python: http://bugs.python.org/issue18879
        self.download(path, local_tmp_file)

        return _DeleteOnCloseFile(local_tmp_file, mode)

    def copy_from_local_file(self, local_path, dest, overwrite=False, **kwargs):
        self.put(local_path, dest, overwrite=overwrite)

    def put(self, local_path, destination, overwrite=False):
        self.upload(destination, local_path, overwrite=overwrite)

    def get(self, path, local_destination):
        self.download(self._remove_schema(path), local_destination)

    def copy(self, path, dest, **kwargs):
        self.move(path, dest)
