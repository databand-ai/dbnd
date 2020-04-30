# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
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
from __future__ import print_function

import bz2
import gzip
import io
import itertools
import os

from errno import EEXIST, EXDEV

import mock
import pytest

import targets
import targets.pipes

from dbnd.testing.helpers_pytest import skip_on_windows
from dbnd_test_scenarios.test_common.targets.base_target_test_mixin import (
    FileTargetTestMixin,
)
from targets import target


class TestLocalTarget(FileTargetTestMixin):
    @pytest.fixture(autouse=True)
    def set_paths_for_test(self, tmpdir, request):
        local_file = str(tmpdir.join("local_file"))
        self.path = local_file
        self.copy = local_file + "-copy-"

    def create_target(self, io_pipe=None):
        return targets.target(self.path, io_pipe=io_pipe)

    def assertCleanUp(self, tmp_path=""):
        if not tmp_path:
            return
        assert not os.path.exists(tmp_path)

    def test_exists(self):
        t = self.create_target()
        p = t.open("w")
        assert t.exists() == os.path.exists(self.path)
        p.close()
        assert t.exists() == os.path.exists(self.path)

    def test_write_simple(self):
        t = self.create_target()
        assert not t.exists()
        p = t.open("w")
        p.write("testtesttest")
        p.close()
        assert t.exists()
        assert open(t.path, "r").read() == "testtesttest"

    def test_write_wrapper(self):
        t = self.create_target()
        assert not t.exists()
        t.write("testtesttest")
        assert t.exists()
        assert open(t.path, "r").read() == "testtesttest"

    @skip_on_windows
    def test_gzip_with_module(self):
        t = target(self.path, io_pipe=targets.pipes.Gzip)
        p = t.open("w")
        test_data = b"test"
        p.write(test_data)
        print(self.path)
        assert not os.path.exists(self.path)
        p.close()
        assert os.path.exists(self.path)

        # Using gzip module as validation
        f = gzip.open(self.path, "r")
        assert test_data == f.read()
        f.close()

        # Verifying our own gzip reader
        f = target(self.path, io_pipe=targets.pipes.Gzip).open("r")
        assert test_data == f.read()
        f.close()

    @skip_on_windows
    def test_bzip2(self):
        t = target(self.path, io_pipe=targets.pipes.Bzip2)
        p = t.open("w")
        test_data = b"test"
        p.write(test_data)
        print(self.path)
        assert not os.path.exists(self.path)
        p.close()
        assert os.path.exists(self.path)

        # Using bzip module as validation
        f = bz2.BZ2File(self.path, "r")
        assert test_data == f.read()
        f.close()

        # Verifying our own bzip2 reader
        f = target(self.path, io_pipe=targets.pipes.Bzip2).open("r")
        assert test_data == f.read()
        f.close()

    def test_copy(self):
        t = target(self.path)
        f = t.open("w")
        test_data = "test"
        f.write(test_data)
        f.close()
        assert os.path.exists(self.path)
        assert not os.path.exists(self.copy)
        t.copy(self.copy)
        assert os.path.exists(self.path)
        assert os.path.exists(self.copy)
        assert t.open("r").read() == target(self.copy).open("r").read()

    def test_move(self):
        t = target(self.path)
        f = t.open("w")
        test_data = "test"
        f.write(test_data)
        f.close()
        assert os.path.exists(self.path)
        assert not os.path.exists(self.copy)
        t.move(self.copy)
        assert not os.path.exists(self.path)
        assert os.path.exists(self.copy)

    def test_move_across_filesystems(self):
        t = target(self.path)
        with t.open("w") as f:
            f.write("test_data")

        def rename_across_filesystems(src, dst):
            err = OSError()
            err.errno = EXDEV
            raise err

        real_rename = os.rename

        def mockrename(src, dst):
            if "-across-fs" in src:
                real_rename(src, dst)
            else:
                rename_across_filesystems(src, dst)

        copy = "%s-across-fs" % self.copy
        with mock.patch("os.rename", mockrename):
            t.move(copy)

        assert not os.path.exists(self.path)
        assert os.path.exists(copy)
        assert "test_data" == target(copy).open("r").read()

    @skip_on_windows
    def test_format_chain(self):
        UTF8WIN = targets.pipes.TextPipeline(encoding="utf8", newline="\r\n")
        t = target(self.path, io_pipe=UTF8WIN >> targets.pipes.Gzip)
        a = u"我é\nçф"

        with t.open("w") as f:
            f.write(a)

        f = gzip.open(self.path, "rb")
        b = f.read()
        f.close()

        assert b"\xe6\x88\x91\xc3\xa9\r\n\xc3\xa7\xd1\x84" == b

    @skip_on_windows
    def test_format_chain_reverse(self):
        t = target(self.path, io_pipe=targets.pipes.UTF8 >> targets.pipes.Gzip)

        f = gzip.open(self.path, "wb")
        f.write(b"\xe6\x88\x91\xc3\xa9\r\n\xc3\xa7\xd1\x84")
        f.close()

        with t.open("r") as f:
            b = f.read()

        assert u"我é\nçф" == b

    @mock.patch("os.linesep", "\r\n")
    def test_format_newline(self):
        t = target(self.path, io_pipe=targets.pipes.SysNewLine)

        with t.open("w") as f:
            f.write(b"a\rb\nc\r\nd")

        with t.open("r") as f:
            b = f.read()

        with open(self.path, "rb") as f:
            c = f.read()

        assert b"a\nb\nc\nd" == b
        assert b"a\r\nb\r\nc\r\nd" == c

    def theoretical_io_modes(self, rwax="rwax", bt=["", "b", "t"], plus=["", "+"]):
        p = itertools.product(rwax, plus, bt)
        return {
            "".join(c)
            for c in list(
                itertools.chain.from_iterable([itertools.permutations(m) for m in p])
            )
        }

    def valid_io_modes(self, *a, **kw):
        modes = set()
        t = self.create_target()
        t.open("w").close()
        for mode in self.theoretical_io_modes(*a, **kw):
            try:
                io.FileIO(t.path, mode).close()
            except ValueError:
                pass
            except IOError as err:
                if err.errno == EEXIST:
                    modes.add(mode)
                else:
                    raise
            else:
                modes.add(mode)
        return modes

    def valid_write_io_modes_for_targets(self):
        return self.valid_io_modes("w", plus=[""])

    def valid_read_io_modes_for_targets(self):
        return self.valid_io_modes("r", plus=[""])

    def invalid_io_modes_for_targets(self):
        return self.valid_io_modes().difference(
            self.valid_write_io_modes_for_targets(),
            self.valid_read_io_modes_for_targets(),
        )

    def test_open_modes(self, tmpdir):
        t = targets.target(str(tmpdir.join("some_file")))
        print("Valid write mode:", end=" ")
        for mode in self.valid_write_io_modes_for_targets():
            print(mode, end=" ")
            p = t.open(mode)
            p.close()
        print()
        print("Valid read mode:", end=" ")
        for mode in self.valid_read_io_modes_for_targets():
            print(mode, end=" ")
            p = t.open(mode)
            p.close()
        print()
        print("Invalid mode:", end=" ")
        # for mode in self.invalid_io_modes_for_targets():
        #     print(mode, end=" ")
        #     with pytest.raises(Exception):
        #         t.open(mode)
        print()

    def test_path_with_file_pattern(self):
        t = targets.target(self.path, "a/1")

        t_star = target(self.path + "/a/*")
        assert not t_star.exists()
        t.touch()
        assert t_star.exists()


class LocalTargetRelativeTest(TestLocalTarget):
    # We had a bug that caused relative file paths to fail, adding test for it
    path = "test.txt"
    copy = "copy.txt"
