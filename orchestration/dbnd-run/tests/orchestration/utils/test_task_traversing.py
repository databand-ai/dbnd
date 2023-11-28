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

from typing import List

from dbnd import output
from dbnd._core.utils.traversing import getpaths, traverse
from dbnd_run.task.pipeline_task import PipelineTask
from dbnd_run.tasks import Task
from dbnd_run.testing.helpers import TTask
from targets import Target, target


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

    def test_flattern_file_target(self):

        from dbnd_run.task_ctrl.task_relations import _find_target

        nested_v = target("/tmp")
        value = {"a": {"b": nested_v}}
        actual = traverse(value, convert_f=_find_target, filter_none=True)
        assert actual
        assert actual.get("a").get("b") == nested_v

    def test_deps_building(self):
        class P1(PipelineTask):
            def band(self):
                return TTask(task_name=self.task_name + "_sub_task")

        def force_pipline_dependency(
            dependee: PipelineTask, depend_on: List[PipelineTask]
        ):
            """
            Force a dbnd pipeline to run after other pipelines, even if they do not naturally depend on one another.
            Can depend on one or more pipelines, waiting for all to complete before starting the dependee
            """
            for prerequisite in depend_on:
                dependee.set_upstream(prerequisite)
                for descendant in dependee.descendants.get_children():
                    descendant.set_upstream(prerequisite)

        p = P1(task_name="p_wait_for_all")
        depend_on = [P1(task_name="d1"), P1(task_name="d2")]

        force_pipline_dependency(p, depend_on)
        p.dbnd_run()
