# © Copyright Databand.ai, an IBM Company 2022

from typing import Dict, List

import pytest

from dbnd import parameter
from dbnd._core.parameter.parameter_value import ParameterValue, fold_parameter_value


list_of_ints = parameter[List[int]].build_parameter("context")
str_to_init_map = parameter[Dict[str, int]].build_parameter("context")
just_int = parameter[int].build_parameter("context")


@pytest.mark.parametrize(
    "left, right, expected",
    [
        pytest.param(
            ParameterValue(
                parameter=list_of_ints, value=[1], source="test", source_value=[1]
            ),
            None,
            ParameterValue(
                parameter=list_of_ints, value=[1], source="test", source_value=[1]
            ),
            id="Fold value with None",
        ),
        pytest.param(
            ParameterValue(
                parameter=list_of_ints, value=[1], source="test", source_value=[1]
            ),
            ParameterValue(
                parameter=list_of_ints, value=[2], source="test_1", source_value=[2]
            ),
            ParameterValue(
                parameter=list_of_ints,
                value=[1, 2],
                source="test,test_1",
                source_value=[1],
            ),
            id="Fold list with list ",
        ),
        pytest.param(
            ParameterValue(
                parameter=str_to_init_map,
                value={"1": 1},
                source="test",
                source_value={"1": 1},
            ),
            ParameterValue(
                parameter=str_to_init_map,
                value={"1": 2},
                source="test_1",
                source_value={"1": 1},
            ),
            ParameterValue(
                parameter=str_to_init_map,
                value={"1": 2},
                source="test,test_1",
                source_value={"1": 1},
            ),
            id="Fold dict with dict which override",
        ),
        pytest.param(
            ParameterValue(
                parameter=str_to_init_map,
                value={"1": 1},
                source="test",
                source_value={"1": 1},
            ),
            ParameterValue(
                parameter=str_to_init_map,
                value={"2": 2},
                source="test_1",
                source_value={"1": 1},
            ),
            ParameterValue(
                parameter=str_to_init_map,
                value={"1": 1, "2": 2},
                source="test,test_1",
                source_value={"1": 1},
            ),
            id="Fold dict with dict",
        ),
        pytest.param(
            ParameterValue(
                parameter=str_to_init_map,
                value={"1": 1},
                source="test",
                source_value={"1": 1},
                warnings=["couldn't parse part"],
            ),
            ParameterValue(
                parameter=str_to_init_map,
                value={"2": 2},
                source="test_1",
                source_value={"1": 1},
                warnings=["Im just sad"],
            ),
            ParameterValue(
                parameter=str_to_init_map,
                value={"1": 1, "2": 2},
                source="test,test_1",
                source_value={"1": 1},
                warnings=["couldn't parse part", "Im just sad"],
            ),
            id="Fold dict with dict with warnings",
        ),
    ],
)
def test_fold_parameter_value(left, right, expected):
    assert fold_parameter_value(left, right) == expected


@pytest.mark.parametrize(
    "left,right",
    [
        pytest.param(
            ParameterValue(
                parameter=just_int, value=1, source="test", source_value="1"
            ),
            ParameterValue(
                parameter=str_to_init_map,
                value={"2": 2},
                source="test_1",
                source_value={"1": 1},
            ),
            id="left not support merge",
        ),
        pytest.param(
            ParameterValue(
                parameter=just_int, value=1, source="test", source_value="1"
            ),
            ParameterValue(
                parameter=just_int, value=1, source="test", source_value="1"
            ),
            id="left and right not support merge",
        ),
        pytest.param(
            ParameterValue(
                parameter=list_of_ints, value=[1], source="test", source_value=[1]
            ),
            ParameterValue(
                parameter=str_to_init_map,
                value={"2": 2},
                source="test_1",
                source_value={"1": 1},
            ),
            id="left and right are different",
        ),
    ],
)
def test_fold_parameter_value_raises(left, right):
    with pytest.raises(ValueError):
        fold_parameter_value(left, right)
