# -*- coding: utf-8 -*-
#
# Copyright 2015 VNG Corporation
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

from dbnd import Config, parameter
from targets import FileSystem
from targets.config import get_local_tempfile
from targets.utils.atomic import _DeleteOnCloseFile


HDFS_SCHEMA = "hdfs://"


class WebHdfsClientType(object):
    INSECURE = "insecure"
    KERBEROS = "kerberos"
    TOKEN = "token"
    # KNOX = "knox"


class KerberosMutualAuthentication(object):
    REQUIRED = "REQUIRED"
    OPTIONAL = "OPTIONAL"
    DISABLED = "DISABLED"


class HdfsCli(FileSystem, Config):
    _conf__task_family = "hdfs_cli"

    host = parameter.c(description="HDFS name node URL", default="localhost")[str]
    port = parameter.c(description="HDFS name node port", default=50070)[int]
    user = parameter.c(default="root")[str]

    token = parameter.c(default=None)[str]

    client_type = parameter.c(
        default=WebHdfsClientType.INSECURE, description="Type of client used to HDFS"
    )

    mutual_authentication = parameter.c(default=KerberosMutualAuthentication.REQUIRED)[
        str
    ]

    service = parameter.c(default="HTTP")[str]
    delegate = parameter.c(default=False)[bool]
    force_preemptive = parameter.c(default=False)[bool]
    principal = parameter.c(default=None)[str]
    hostname_override = parameter.c(default=None)[str]
    sanitize_mutual_error_response = parameter.c(default=True)[bool]
    send_cbt = parameter.c(default=True)[bool]

    @staticmethod
    def _remove_schema(path):
        if path.startswith(HDFS_SCHEMA):
            return path[7:]
        return path

    @property
    def url(self):
        # the hdfs package allows it to specify multiple namenodes by passing a string containing
        # multiple namenodes separated by ';'
        hosts = self.host.split(";")
        urls = ["http://" + host + ":" + str(self.port) for host in hosts]
        return ";".join(urls)

    @property
    def client(self):  # type ()-> WebHDFS
        if self.client_type == WebHdfsClientType.KERBEROS:
            from hdfs.ext.kerberos import KerberosClient

            return KerberosClient(
                url=self.url,
                mutual_authentication=self.mutual_authentication,
                service=self.service,
                delegate=self.delegate,
                force_preemptive=self.force_preemptive,
                principal=self.principal,
                hostname_override=self.hostname_override,
                sanitize_mutual_error_response=self.sanitize_mutual_error_response,
                send_cbt=self.send_cbt,
            )

        elif self.client_type == WebHdfsClientType.INSECURE:
            from hdfs import InsecureClient

            return InsecureClient(url=self.url, user=self.user)

        elif self.client_type == WebHdfsClientType.TOKEN:
            from hdfs import TokenClient

            return TokenClient(url=self.url, token=self.token)
        else:
            raise Exception("WebHdfs client type %s does not exist" % self.client_type)

    def walk(self, path, depth=1):
        return self.client.walk(path, depth=depth)

    def exists(self, path):
        from hdfs.util import HdfsError

        try:
            self.client.status(self._remove_schema(path))
            return True

        except HdfsError as e:
            if str(e).startswith("File does not exist: "):
                return False
            else:
                raise e

    def put_string(self, content, hdfs_path, **kwargs):
        return self.client.write(self._remove_schema(hdfs_path), content, kwargs)

    def upload(
        self, hdfs_path, local_path, overwrite=False, force=False, recursive=False
    ):
        return self.client.upload(
            self._remove_schema(hdfs_path), local_path, overwrite=overwrite
        )

    def download_file(
        self, hdfs_path, local_path, overwrite=False, n_threads=-1, **kwargs
    ):
        return self.client.download(
            self._remove_schema(hdfs_path),
            local_path,
            overwrite=overwrite,
            n_threads=n_threads,
        )

    def remove(self, hdfs_path, recursive=True, skip_trash=True):
        return self.client.delete(self._remove_schema(hdfs_path), recursive=recursive)

    def read(
        self,
        hdfs_path,
        offset=0,
        length=None,
        buffer_size=None,
        chunk_size=1024,
        buffer_char=None,
    ):
        return self.client.read(
            self._remove_schema(hdfs_path),
            offset=offset,
            length=length,
            buffer_size=buffer_size,
            chunk_size=chunk_size,
            buffer_char=buffer_char,
        )

    def move(self, path, dest):
        path = self._remove_schema(path)
        dest = self._remove_schema(dest)
        parts = dest.rstrip("/").split("/")
        if len(parts) > 1:
            dir_path = "/".join(parts[0:-1])
            if not self.exists(dir_path):
                self.mkdir(dir_path, parents=True)
        self.client.rename(path, dest)

    def mkdir(self, path, parents=True, mode=0o755, raise_if_exists=False):
        """
        Has no returnvalue (just like WebHDFS)
        """
        if not parents or raise_if_exists:
            warnings.warn("webhdfs mkdir: parents/raise_if_exists not implemented")
        permission = int(oct(mode)[2:])  # Convert from int(decimal) to int(octal)
        self.client.makedirs(self._remove_schema(path), permission=permission)

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
        for obj in self.client.list(self._remove_schema(path), status=False):
            yield os.path.join(path, obj)

    def isdir(self, path):
        from hdfs.util import HdfsError

        try:
            status = self.client.status(self._remove_schema(path))
            if not status:
                return False
            return status["type"] == "DIRECTORY"
        except HdfsError as e:
            if str(e).startswith("File does not exist: "):
                return False
            else:
                raise e

    def get_as_string(self, hdfs_path):
        with self.client.read(self._remove_schema(hdfs_path)) as reader:
            content = reader.read()
        return content

    def open_read(self, path, mode="r"):
        local_tmp_file = get_local_tempfile("download-%s" % os.path.basename(path))
        # We can't return the tempfile reference because of a bug in python: http://bugs.python.org/issue18879
        self.download(path, local_tmp_file)

        return _DeleteOnCloseFile(local_tmp_file, mode)

    def copy_from_local_file(self, local_path, dest, **kwargs):
        self.put(local_path, dest)

    def put(self, local_path, destination):
        self.upload(destination, local_path)

    def get(self, path, local_destination):
        self.download(self._remove_schema(path), local_destination)

    def copy(self, path, dest, **kwargs):
        self.move(path, dest)
