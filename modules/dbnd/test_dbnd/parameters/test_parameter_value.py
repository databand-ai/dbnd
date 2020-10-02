from typing import Dict, List

from dbnd import parameter
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from targets import Target, target
from targets.types import Path
from targets.values import TargetPathLibValueType
from targets.values.structure import _StructureValueType


class TestParameterValue(object):
    def test_value_type_list(self):
        p = parameter[List[Path]]._p
        assert isinstance(p.value_type, _StructureValueType)
        assert isinstance(p.value_type.sub_value_type, TargetPathLibValueType)

    def test_list_parse(self):
        p = parameter[List[Path]]._p  # type: ParameterDefinition
        data = [target("/a"), target("/b")]
        actual = p.calc_init_value(data)

        for l in actual:
            assert isinstance(l, Target)

        runtime_value = p.calc_runtime_value(actual, None)

        for l in runtime_value:
            assert isinstance(l, Path)

    def test_dict_parse(self):
        p = parameter[Dict[str, Path]]._p  # type: ParameterDefinition
        data = {"a": target("/a"), "b": target("/b")}
        actual = p.calc_init_value(data)

        for k, v in actual.items():
            assert isinstance(v, Target)

        runtime_value = p.calc_runtime_value(actual, None)

        for k, v in runtime_value.items():
            assert isinstance(v, Path)
