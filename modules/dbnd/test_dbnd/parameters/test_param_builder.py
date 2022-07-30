# © Copyright Databand.ai, an IBM Company 2022

from dbnd import output
from targets.target_config import file


class TestParameterBuilder(object):
    def test_builder(self):
        target = output(name="ttt")
        assert target.parameter.name == "ttt"
        assert target.parameter.target_config != file.feather

        target = target.feather()

        assert target.parameter.target_config == file.feather
        assert target.parameter.name

    def test_build_parameter(self):
        actual = output(name="ttt")._p

        assert actual
        assert actual.name == "ttt"
