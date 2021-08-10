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

import pytest

from dbnd import parameter
from dbnd._core.errors import ParseParameterError


class TestChoiceParameter(object):
    def test_parse_str(self):
        d = parameter.choices(["1", "2", "3"])[str]._p
        assert "3" == d.parse_from_str("3")

    def test_parse_int(self):
        d = parameter.choices([1, 2, 3])[int]._p
        assert 3 == d.parse_from_str(3)

    def test_parse_int_conv(self):
        d = parameter.choices([1, 2, 3])[int]._p
        assert 3 == d.parse_from_str("3")

    def test_invalid_choice(self):
        d = parameter.choices(["1", "2", "3"])[str]._p
        with pytest.raises(ParseParameterError):
            d.validate("xyz")

    def test_invalid_choice_type(self):
        with pytest.raises(AssertionError):
            parameter.choices([1, 2, "3"])[int]._p.validate(2)

    def test_serialize_parse(self):
        a = parameter.choices(["1", "2", "3"])[str]._p
        b = "3"
        assert b == a.parse_from_str(a.to_str(b))
