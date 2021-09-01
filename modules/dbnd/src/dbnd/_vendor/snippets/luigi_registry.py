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
from dbnd._vendor.snippets.edit_distance import get_editdistance


def module_parents(module_name):
    """
    >>> list(module_parents('a.b'))
    ['a.b', 'a', '']
    """
    spl = module_name.split(".")
    for i in range(len(spl), 0, -1):
        yield ".".join(spl[0:i])
    if module_name:
        yield ""


def get_best_candidate(task_name, known_task_names, max_dist=5):
    weighted_tasks = [
        (get_editdistance(str(task_name), known_task_name), known_task_name)
        for known_task_name in known_task_names
    ]
    ordered_tasks = sorted(weighted_tasks, key=lambda pair: pair[0])
    return [
        task for (dist, task) in ordered_tasks if dist <= max_dist and dist < len(task)
    ]
