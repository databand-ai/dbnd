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
#
# This file has been modified by databand.ai to support dbnd orchestration.

import collections
import json
import logging

from typing import List

import pytest

from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase
from targets.values import (
    DictValueType,
    IntValueType,
    ListValueType,
    SetValueType,
    ValueType,
)


class CustomValueType(ValueType):
    type = str

    def parse_from_str(self, x):
        return "__" + x

    def to_str(self, x):
        return x[2:]


class TestListParameter(TargetTestBase):
    _list = [["username", "me"], ["password", "secret"]]
    _list_ints = [3]
    _list_custom = ["3"]

    def test_parse_simple(self):
        assert self._list == ListValueType().parse_from_str(json.dumps(self._list))

    def test_parse_custom(self):
        actual = ListValueType(sub_value_type=CustomValueType()).parse_from_str('["3"]')
        assert ["__3"] == actual

    def test_parse_custom_user(self):
        actual = ListValueType(sub_value_type=CustomValueType()).parse_from_str("A,B")
        assert ["__A", "__B"] == actual

    def test_dump_to_str_task(self):
        assert '[["username","me"],["password","secret"]]' == ListValueType().to_str(
            self._list
        )
        assert '["3"]' == ListValueType(sub_value_type=IntValueType()).to_str(
            self._list_ints
        )

    def test_dump_list_int(self):
        p_list = self.target("p_list.csv")
        data = [1, 2]
        p_list.dump(data, value_type=List[int])
        logging.info("Saved data to %s", p_list)
        actual = p_list.load(List[int])
        assert actual == data

    def test_parse_invalid_input(self):
        with pytest.raises(ValueError):
            ListValueType().parse_from_str('{"invalid"}')


class TestDictParameter(object):
    _dict = collections.OrderedDict([("username", "me"), ("password", "secret")])

    def test_parse(self):
        d = DictValueType().parse_from_str(json.dumps(TestDictParameter._dict))
        assert d == TestDictParameter._dict

    def test_parse_and_dump_to_str(self):
        inputs = [
            '{"username": "me", "password": "secret"}',  # pragma: allowlist secret
            '{"password": "secret", "username": "me"}',  # pragma: allowlist secret
        ]
        for json_input in inputs:
            _dict = DictValueType().parse_from_str(json_input)
            assert json.loads(json_input) == _dict

    def test_parse_invalid_input_1(self):
        with pytest.raises(ValueError):
            DictValueType().parse_from_str('{"invalid"}')

    def test_parse_invalid_input_2(self):
        with pytest.raises(ValueError):
            DictValueType().parse_from_str("invalid")


class TestSetParameter(object):
    def test_parse_simple(self):
        assert {"1", "2", "3"} == SetValueType().parse_from_str("1,2,3")

    def test_dump_to_str_task(self):
        p = SetValueType()
        assert p.to_str({1, 2, 3}) == p.to_str({2, 1, 3})
