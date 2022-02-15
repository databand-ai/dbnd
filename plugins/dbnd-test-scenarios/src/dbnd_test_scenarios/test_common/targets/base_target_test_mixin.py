# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

import abc
import random

import pandas as pd
import pytest

from pandas.util.testing import assert_frame_equal
from pytest import fixture

import targets
import targets.errors
import targets.pipes

from dbnd.testing.helpers_pytest import skip_on_windows
from targets import DataTarget


class TestException(Exception):
    pass


class FileTargetTestMixin(object):
    """All Target that take bytes (python2: str) should pass those
    tests. In addition, a test to verify the method `exists`should be added
    """

    @fixture(autouse=True)
    def _current_request(self, request):
        self._current_request = request
        self._id = request.node.name

    def assertCleanUp(self, tmp_path=""):
        pass

    @abc.abstractmethod
    def create_target(self, io_pipe=None):
        # type: (...)->DataTarget
        pass

    def test_atomicity(self):
        target = self.create_target()

        fobj = target.open("w")
        assert not target.exists()
        fobj.close()
        assert target.exists()

    def test_readback(self):
        target = self.create_target()

        origdata = "lol\n"
        fobj = target.open("w")
        fobj.write(origdata)
        fobj.close()

        fobj = target.open("r")
        data = fobj.read()
        assert origdata == data

    def test_unicode_obj(self):
        target = self.create_target()

        origdata = "lol\n"
        fobj = target.open("w")
        fobj.write(origdata)
        fobj.close()
        fobj = target.open("r")
        data = fobj.read()
        assert origdata == data

    def test_with_close(self):
        target = self.create_target()

        with target.open("w") as fobj:
            tp = getattr(fobj, "tmp_path", "")
            fobj.write("hej\n")

        self.assertCleanUp(tp)
        assert target.exists()

    def test_with_exception(self):
        target = self.create_target()

        a = {}

        def foo():
            with target.open("w") as fobj:
                fobj.write("hej\n")
                a["tp"] = getattr(fobj, "tmp_path", "")
                raise TestException("Test triggered exception")

        with pytest.raises(TestException):
            foo()
        self.assertCleanUp(a["tp"])
        assert not target.exists()

    def test_del(self):
        t = self.create_target()
        p = t.open("w")
        print("test", file=p)
        tp = getattr(p, "tmp_path", "")
        del p

        self.assertCleanUp(tp)
        assert not t.exists()

    def test_write_cleanup_no_close(self):
        t = self.create_target()

        def context():
            f = t.open("w")
            f.write("stuff")
            return getattr(f, "tmp_path", "")

        tp = context()
        import gc

        gc.collect()  # force garbage collection of f variable
        self.assertCleanUp(tp)
        assert not t.exists()

    def test_text(self):
        t = self.create_target(targets.pipes.UTF8)
        a = "我éçф"
        with t.open("w") as f:
            f.write(a)
        with t.open("r") as f:
            b = f.read()
        assert a == b

    def test_del_with_Text(self):
        t = self.create_target(targets.pipes.UTF8)
        p = t.open("w")
        print("test", file=p)
        tp = getattr(p, "tmp_path", "")
        del p

        self.assertCleanUp(tp)
        assert not t.exists()

    def test_format_injection(self):
        class CustomFormat(targets.pipes.IOPipeline):
            def pipe_reader(self, input_pipe):
                input_pipe.foo = "custom read property"
                return input_pipe

            def pipe_writer(self, output_pipe):
                output_pipe.foo = "custom write property"
                return output_pipe

        t = self.create_target(CustomFormat())
        with t.open("w") as f:
            assert f.foo == "custom write property"

        with t.open("r") as f:
            assert f.foo == "custom read property"

    def test_binary_write(self):
        t = self.create_target(targets.pipes.Nop)
        with t.open("w") as f:
            f.write(b"a\xf2\xf3\r\nfd")

        with t.open("r") as f:
            c = f.read()

        assert c == b"a\xf2\xf3\r\nfd"

    def test_writelines(self):
        t = self.create_target()
        with t.open("w") as f:
            f.writelines(["a\n", "b\n", "c\n"])

        with t.open("r") as f:
            c = f.read()

        assert c == "a\nb\nc\n"

    def test_read_iterator(self):
        t = self.create_target()
        with t.open("w") as f:
            f.write("a\nb\nc\n")

        c = []
        with t.open("r") as f:
            for x in f:
                c.append(x)

        assert c == ["a\n", "b\n", "c\n"]

    @skip_on_windows
    def test_gzip(self):
        t = self.create_target(io_pipe=targets.pipes.Gzip)
        p = t.open("w")
        test_data = b"test"
        p.write(test_data)
        # tp = getattr(p, "tmp_path", "")
        assert not t.exists()
        p.close()
        # self.assertCleanUp(tp)
        assert t.exists()

    @skip_on_windows
    def test_gzip_works_and_cleans_up(self):
        t = self.create_target(targets.pipes.Gzip)

        test_data = b"123testing"
        with t.open("w") as f:
            tp = getattr(f, "tmp_path", "")
            f.write(test_data)

        self.assertCleanUp(tp)
        with t.open() as f:
            result = f.read()

        assert test_data == result

    @pytest.mark.skip
    def test_dataframe_csv_support(self):
        t = self.create_target()

        test_data = pd.DataFrame(data=[[1, 1], [2, 2]], columns=["c1", "c2"])
        t.as_pandas.to_csv(test_data)

        result = t.as_pandas.read_csv()
        assert_frame_equal(test_data, result)

    @pytest.mark.skip
    def test_dataframe_parquet_support(self):
        t = self.create_target()

        test_data = pd.DataFrame(data=[[1, 1], [2, 2]], columns=["c1", "c2"])
        t.as_pandas.to_parquet(test_data)

        result = t.as_pandas.read_parquet()
        assert_frame_equal(test_data, result)

    def test_move_on_fs(self):
        # We're cheating and retrieving the fs from target.
        # TODO: maybe move to "filesystem_test.py" or something
        t = self.create_target()
        other_path = t.path + "-" + str(random.randint(0, 999999999))
        t.touch()
        fs = t.fs
        assert t.exists()
        fs.move(t.path, other_path)
        assert not t.exists()

    def test_rename_dont_move_on_fs(self):
        # We're cheating and retrieving the fs from target.
        # TODO: maybe move to "filesystem_test.py" or something
        t = self.create_target()
        other_path = t.path + "-" + str(random.randint(0, 999999999))
        t.touch()
        fs = t.fs
        assert t.exists()
        fs.rename_dont_move(t.path, other_path)
        assert not t.exists()
        with pytest.raises(targets.errors.FileAlreadyExists):
            fs.rename_dont_move(t.path, other_path)
