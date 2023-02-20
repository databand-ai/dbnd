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

import json

from typing import List

import pytest

from dbnd import parameter
from dbnd.testing.helpers import build_task
from dbnd.testing.orchestration_utils import TTask
from targets.values import ValueType


class CustomValueType(ValueType):
    def parse_from_str(self, x):
        return "__" + x

    def to_str(self, x):
        return x[2:]


class ListParameterTask(TTask):
    param = parameter[List]
    param_typed = parameter[List[int]]
    param_custom_typed = parameter[List[CustomValueType]]


class TestListParameter(object):
    _list = [["username", "me"], ["password", "secret"]]
    _list_ints = [3]
    _list_custom = ["3"]

    def test_parse_simple(self):
        assert self._list == parameter[List]._p.parse_from_str(json.dumps(self._list))

    def test_parse_int(self):
        actual = parameter[List[int]]._p.parse_from_str('["3"]')
        assert [3] == actual

    def test_parse_custom(self):
        actual = parameter[List[CustomValueType()]]._p.parse_from_str('["3"]')
        assert ["__3"] == actual

    def test_parse_custom_user(self):
        actual = parameter[List[CustomValueType()]]._p.parse_from_str("A,B")
        assert ["__A", "__B"] == actual

    def test_parse_interface(self):
        task = build_task(
            "ListParameterTask",
            param="[1,2]",
            param_typed="['2','3']",
            param_custom_typed="A,B",
        )

        assert task.param == [1, 2]
        assert task.param_typed == [2, 3]
        assert task.param_custom_typed == ["__A", "__B"]

    def test_serialize_task(self):
        assert '[["username","me"],["password","secret"]]' == parameter[List]._p.to_str(
            self._list
        )
        assert '["3"]' == parameter[List[int]]._p.to_str(self._list_ints)

    def test_parse_invalid_input(self):
        with pytest.raises(ValueError):
            parameter[List]._p.parse_from_str('{"invalid"}')
