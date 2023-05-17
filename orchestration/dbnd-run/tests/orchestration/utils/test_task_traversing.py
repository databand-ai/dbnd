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

from dbnd import output
from dbnd._core.utils.traversing import getpaths
from dbnd.tasks import Task
from targets import Target


class TestTraversing(object):
    def test_getpaths(self):
        class RequiredTask(Task):
            t_output = output(default="/path/to/target/file")

        t = RequiredTask()
        reqs = {}
        reqs["bare"] = t
        reqs["dict"] = {"key": t}
        reqs["OrderedDict"] = collections.OrderedDict([("key", t)])
        reqs["list"] = [t]
        reqs["tuple"] = (t,)
        reqs["generator"] = (t for _ in range(10))

        struct = getpaths(reqs)
        assert isinstance(struct, dict)
        assert isinstance(struct["bare"]["t_output"], Target)
        assert isinstance(struct["dict"], dict)
        assert isinstance(struct["OrderedDict"], collections.OrderedDict)
        assert isinstance(struct["list"], list)
        assert isinstance(struct["tuple"], tuple)
        assert hasattr(struct["generator"], "__iter__")
