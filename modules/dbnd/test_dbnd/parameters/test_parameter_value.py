# © Copyright Databand.ai, an IBM Company 2022

from typing import Dict, List

import pytest

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

    @pytest.mark.parametrize(
        "param_def, data, expected",
        [
            (parameter[List[List[int]]], [[1], [1, 2, 3]], [[1], [1, 2, 3]]),
            (parameter[List[str]], [["a"], ["a", "b"]], [["a"], ["a", "b"]]),
            (parameter[List[List[str]]], [["a"], ["a", "b"]], [["a"], ["a", "b"]]),
            (parameter[Dict[str, str]], {"a": "A", "b": "B"}, {"a": "A", "b": "B"}),
            (
                parameter[Dict[str, Dict[str, str]]],
                {"capitals": {"a": "A", "b": "B"}},
                {"capitals": {"a": "A", "b": "B"}},
            ),
        ],
    )
    def test_parsing_sub_structure(self, param_def, data, expected):
        p = param_def._p  # type: ParameterDefinition
        actual = p.calc_init_value(data)
        assert actual == expected
