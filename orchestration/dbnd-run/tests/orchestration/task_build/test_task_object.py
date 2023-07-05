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
# This file has been modified by databand.ai to support dbnd orchestration.

from __future__ import absolute_import

import copy
import doctest
import pickle
import warnings

from datetime import date, datetime, timedelta

import pytz

from databand.parameters import DateHourParameter, TimeDeltaParameter
from dbnd import data, output, parameter
from dbnd._core.task import base_task
from dbnd_run.tasks import DataSourceTask, Task
from dbnd_test_scenarios.test_common.task.factories import TTask
from targets import target


class DefaultInsignificantParamTask(TTask):
    insignificant_param = parameter.value(significant=False, default="value")
    necessary_param = parameter.value(significant=False)[str]


class MyExternalTask(DataSourceTask):
    some_outputs = output

    def band(self):
        self.some_outputs = target("/tmp")


class TestTaskObject(object):
    def test_task_deepcopy(self, tmpdir_factory):
        class TestTask(Task):
            test_input = data
            p = parameter[str]
            d = parameter[date]
            param_from_config = parameter[date]

            a_output = output

        actual = TestTask(test_input="sss", p="333", d=date(2018, 3, 4))

        copy.deepcopy(actual)
        assert actual.p == "333"

    def test_tasks_doctest(self):
        doctest.testmod(base_task)

    def test_external_tasks_loadable(self):
        task = MyExternalTask()
        assert isinstance(task, DataSourceTask)

    def test_no_warn_if_param_types_ok(self):
        class DummyTask(TTask):
            param = parameter[str]
            bool_param = parameter[bool]
            int_param = parameter[int]
            float_param = parameter[float]
            date_param = parameter[datetime]
            datehour_param = DateHourParameter()
            timedelta_param = TimeDeltaParameter()
            insignificant_param = parameter(significant=False)[str]

        DUMMY_TASK_OK_PARAMS = dict(
            param="test",
            bool_param=True,
            int_param=666,
            float_param=123.456,
            date_param=datetime(2014, 9, 13).date(),
            datehour_param=datetime(2014, 9, 13, 9, tzinfo=pytz.UTC),
            timedelta_param=timedelta(44),  # doesn't support seconds
            insignificant_param="test",
        )

        with warnings.catch_warnings(record=True) as w:
            DummyTask(**DUMMY_TASK_OK_PARAMS)
        print([str(wm) for wm in w])
        assert (
            len(w) == 0
        ), "No warning should be raised when correct parameter types are used"

    def test_task_caching(self):
        class DummyTask(Task):
            x = parameter[str]

        dummy_1 = DummyTask(x=1)
        dummy_2 = DummyTask(x=2)
        dummy_1b = DummyTask(x=1)

        assert dummy_1 != dummy_2
        assert dummy_1 == dummy_1b

    def test_task_obj_serializable(self):
        t = TTask()
        pickled = pickle.dumps(t)
        assert t == pickle.loads(pickled)
