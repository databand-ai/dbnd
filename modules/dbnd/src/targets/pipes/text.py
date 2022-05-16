# -*- coding: utf-8 -*-
#
# Copyright 2015 Spotify AB
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
# This file has been modified by databand.ai to support dbnd orchestration and tracking.


import io
import locale
import os
import re
import warnings

import six

from targets.pipes.base import BaseWrapper, WrappedFormat


class NewlineWrapper(BaseWrapper):
    def __init__(self, stream, newline=None):
        if newline is None:
            self.newline = newline
        else:
            self.newline = newline.encode("ascii")

        if self.newline not in (b"", b"\r\n", b"\n", b"\r", None):
            raise ValueError(
                "newline need to be one of {b'', b'\r\n', b'\n', b'\r', None}"
            )
        super(NewlineWrapper, self).__init__(stream)

    def read(self, n=-1):
        b = self._stream.read(n)

        if self.newline == b"":
            return b

        if self.newline is None:
            newline = b"\n"

        return re.sub(b"(\n|\r\n|\r)", newline, b)

    def writelines(self, lines):
        if self.newline is None or self.newline == "":
            newline = os.linesep.encode("ascii")
        else:
            newline = self.newline

        self._stream.writelines(
            (re.sub(b"(\n|\r\n|\r)", newline, line) for line in lines)
        )

    def write(self, b):
        if self.newline is None or self.newline == "":
            newline = os.linesep.encode("ascii")
        else:
            newline = self.newline

        self._stream.write(re.sub(b"(\n|\r\n|\r)", newline, b))


class TextWrapper(io.TextIOWrapper):
    def __exit__(self, *args):
        # io.TextIOWrapper close the file on __exit__, let the underlying file decide
        if not self.closed and self.writable():
            super(TextWrapper, self).flush()

        self._stream.__exit__(*args)

    def __del__(self, *args):
        # io.TextIOWrapper close the file on __del__, let the underlying file decide
        if not self.closed and self.writable():
            super(TextWrapper, self).flush()

        try:
            self._stream.__del__(*args)
        except AttributeError:
            pass

    def __init__(self, stream, *args, **kwargs):
        self._stream = stream
        try:
            super(TextWrapper, self).__init__(stream, *args, **kwargs)
        except TypeError:
            pass

    def __getattr__(self, name):
        if name == "_stream":
            raise AttributeError(name)
        return getattr(self._stream, name)

    def __enter__(self):
        self._stream.__enter__()
        return self


class MixedUnicodeBytesWrapper(BaseWrapper):
    """ """

    def __init__(self, stream, encoding=None):
        if encoding is None:
            encoding = locale.getpreferredencoding()
        self.encoding = encoding
        super(MixedUnicodeBytesWrapper, self).__init__(stream)

    def write(self, b):
        self._stream.write(self._convert(b))

    def writelines(self, lines):
        self._stream.writelines((self._convert(line) for line in lines))

    def _convert(self, b):
        if isinstance(b, six.text_type):
            b = b.encode(self.encoding)
            warnings.warn("Writing unicode to byte stream", stacklevel=2)
        return b


class TextPipeline(WrappedFormat):

    input = "unicode"
    output = "bytes"
    wrapper_cls = TextWrapper


class MixedUnicodeBytesFormat(WrappedFormat):

    output = "bytes"
    wrapper_cls = MixedUnicodeBytesWrapper


class NewlinePipeline(WrappedFormat):

    input = "bytes"
    output = "bytes"
    wrapper_cls = NewlineWrapper
