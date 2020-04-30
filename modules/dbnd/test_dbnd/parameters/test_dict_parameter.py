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

import collections
import json

import pytest

from databand import parameters
from dbnd.testing.helpers import build_task
from dbnd_test_scenarios.test_common.task.factories import TTask


class DictParameterTask(TTask):
    param = parameters.DictParameter()


class TestDictParameter(object):
    _dict = collections.OrderedDict([("username", "me"), ("password", "secret")])

    def test_parse(self):
        d = parameters.DictParameter()._p.parse_from_str(
            json.dumps(TestDictParameter._dict)
        )
        assert d == TestDictParameter._dict

    def test_parse_and_serialize(self):
        inputs = [
            '{"username": "me", "password": "secret"}',
            '{"password": "secret", "username": "me"}',
        ]
        for json_input in inputs:
            _dict = parameters.DictParameter()._p.parse_from_str(json_input)
            assert json.loads(json_input) == _dict

    def test_parse_interface(self):
        task = build_task(
            "DictParameterTask", param='{"username": "me", "password": "secret"}'
        )
        assert TestDictParameter._dict == task.param

    def test_parse_invalid_input(self):
        with pytest.raises(ValueError):
            parameters.DictParameter()._p.parse_from_str('{"invalid"}')
