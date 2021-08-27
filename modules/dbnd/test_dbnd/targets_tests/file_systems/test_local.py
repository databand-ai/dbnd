import os
import unittest

import pytest

from targets import FileAlreadyExists, MissingParentDirectory, target
from targets.fs.local import LocalFileSystem


class TestLocalFileSystem(unittest.TestCase):
    # TODO change to test local path
    fs = LocalFileSystem()

    @pytest.fixture(autouse=True)
    def create_test_path(self, tmpdir):
        self.path = str(tmpdir)

    def test_copy(self):
        src = os.path.join(self.path, "src.txt")
        dest = os.path.join(self.path, "newdir", "dest.txt")

        target(src).open("w").close()
        self.fs.copy(src, dest)
        assert os.path.exists(src)
        assert os.path.exists(dest)

    def test_mkdir(self):
        testpath = os.path.join(self.path, "foo/bar")

        with pytest.raises(MissingParentDirectory):
            self.fs.mkdir(testpath, parents=False)

        self.fs.mkdir(testpath)
        assert os.path.exists(testpath)
        assert self.fs.isdir(testpath)

        with pytest.raises(FileAlreadyExists):
            self.fs.mkdir(testpath, raise_if_exists=True)

    def test_exists(self):
        assert self.fs.exists(self.path)
        assert self.fs.isdir(self.path)

    def test_listdir(self):
        with open(self.path + "/file", "w"):
            pass
        assert [self.path + "/file"], list(self.fs.listdir(self.path + "/"))

    def test_move_to_new_dir(self):
        # Regression test for a bug in LocalFileSystem.move
        src = os.path.join(self.path, "src.txt")
        dest = os.path.join(self.path, "newdir", "dest.txt")

        target(src).open("w").close()
        self.fs.move(src, dest)
        assert os.path.exists(dest)
