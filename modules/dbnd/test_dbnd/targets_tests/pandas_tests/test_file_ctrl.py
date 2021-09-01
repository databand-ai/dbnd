from __future__ import print_function

import logging

import pytest

from targets import target


logger = logging.getLogger(__name__)


class TestPickleTargetCtrl(object):
    @pytest.fixture(autouse=True)
    def set_paths_for_test(self, tmpdir):
        local_file = str(tmpdir.join("local_file"))
        self.path = local_file

    def create_target(self, io_pipe=None, **kwargs):
        return target(self.path, io_pipe=io_pipe, **kwargs)

    def test_pickle(self):
        t = self.create_target()

        expected = {"a": "value"}
        t.write_pickle(expected)

        actual = t.read_pickle()
        assert expected == actual

    def test_write(self):
        t = self.create_target()

        expected = "hellow"
        t.write(expected)

        actual = t.read()
        assert expected == actual

    def test_writelines(self):
        t = self.create_target()

        expected = ["hellow", "byebye"]
        t.writelines(expected)

        actual = t.readlines()
        assert expected == actual
