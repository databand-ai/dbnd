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


import pytest

from dbnd._core.task_ctrl.task_relations import _find_target
from dbnd._core.utils import traversing
from dbnd._core.utils.traversing import traverse
from targets import target


class TestTraversing(object):
    def test_flatten(self):
        flatten = traversing.flatten
        assert sorted(flatten({"a": "foo", "b": "bar"})) == ["bar", "foo"]
        assert sorted(flatten(["foo", ["bar", "troll"]])) == ["bar", "foo", "troll"]
        assert flatten("foo") == ["foo"]
        assert flatten(42) == [42]
        assert flatten((len(i) for i in ["foo", "troll"])) == [3, 5]
        pytest.raises(TypeError, flatten, (len(i) for i in ["foo", "troll", None]))

    def test_flattern_file_target(self):
        nested_v = target("/tmp")
        value = {"a": {"b": nested_v}}
        actual = traverse(value, convert_f=_find_target, filter_none=True)
        assert actual
        assert actual.get("a").get("b") == nested_v
