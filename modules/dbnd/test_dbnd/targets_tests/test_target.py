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
from __future__ import print_function

import pytest

import targets
import targets.pipes


class TException(Exception):
    pass


class TestTarget(object):
    def test_cannot_instantiate(self):
        def instantiate_target():
            targets.Target()

        pytest.raises(TypeError, instantiate_target)

    def test_abstract_subclass(self):
        class ExistsLessTarget(targets.Target):
            pass

        def instantiate_target():
            ExistsLessTarget()

        pytest.raises(TypeError, instantiate_target)

    def test_instantiate_subclass(self):
        class GoodTarget(targets.Target):
            def exists(self):
                return True

            def open(self, mode):
                return None

        GoodTarget()
