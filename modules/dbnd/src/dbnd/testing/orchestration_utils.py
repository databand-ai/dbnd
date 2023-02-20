# © Copyright Databand.ai, an IBM Company 2022

from __future__ import absolute_import

import pytest

from pytest import fixture

from dbnd import output, parameter
from dbnd.tasks import PythonTask
from targets import DataTarget, target


class TTask(PythonTask):
    t_param = parameter.value("1")
    t_output = output.data

    def run(self):
        self.t_output.write("%s" % self.t_param)


class TargetTestBase(object):
    @pytest.fixture(autouse=True)
    def _set_temp_dir(self, tmpdir):
        self.tmpdir = tmpdir

    def target(self, *args, **kwargs):
        # type: (...) -> DataTarget
        return target(str(self.tmpdir), *args, **kwargs)

    @fixture
    def target_1_2(self):
        t = self.target("file.txt")
        t.as_object.writelines(["1", "2"])
        return t
