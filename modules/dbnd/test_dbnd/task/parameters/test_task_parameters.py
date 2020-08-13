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
import enum
import logging

from typing import Dict, List, Optional, Tuple

from dbnd import ParameterDefinition, band, config, parameter, task
from dbnd._core.task.task import TASK_PARAMS_COUNT, Task
from dbnd_test_scenarios.test_common.task.factories import TTask


logger = logging.getLogger(__name__)


class TtWithDefault(TTask):
    x = parameter.value(default="xyz")


class Foo(TTask):
    bar = parameter[str]
    p2 = parameter[int]
    not_a_param = "lol"

    _task_params_in_foo = 7


class ListFoo(TTask):
    my_list = parameter.sub_type(int)[List]

    def run(self):
        super(ListFoo, self).run()
        ListFoo._val = self.my_list


class TupleFoo(TTask):
    my_tuple = parameter[Tuple]

    def run(self):
        TupleFoo._val = self.my_tuple


class MyEnum(enum.Enum):
    A = 1


class _CustomType(object):
    pass


class TestTaskParameters(object):
    def test_default_param(self):
        assert TtWithDefault().x == "xyz"

    def test_parameter_registration(self):
        # we have double time of params because of config
        assert len(Foo.task_definition.all_task_params) == (
            Foo._task_params_in_foo + TASK_PARAMS_COUNT
        )

    def test_task_creation(self):
        f = Foo(bar="barval", p2=5)
        assert (
            len(f._params.get_params()) == Foo._task_params_in_foo + TASK_PARAMS_COUNT
        )
        assert f.bar == "barval"
        assert f.p2 == 5
        assert f.not_a_param == "lol"

    def test_list(self):
        with config({"ListFoo": {"my_list": "1,2"}}) as c:
            ListFoo().dbnd_run()
        assert ListFoo._val == [1, 2]

    def test_default_param_cmdline(self):
        assert TtWithDefault().x == "xyz"

    def test_insignificant_parameter(self):
        class InsignificantParameterTask(TTask):
            foo = parameter.value(significant=False, default="foo_default")
            bar = parameter[str]

        t1 = InsignificantParameterTask(foo="x", bar="y")
        assert "foo" not in str(t1)

        t2 = InsignificantParameterTask(foo="u", bar="z")
        assert t2.foo == "u"
        assert t2.bar == "z"
        assert "foo" not in str(t2)

    def test_environ_parameter_in_default(self):
        """ Obviously, if anything should be positional, so should local
        significant parameters """

        import os

        os.environ["ENV_TEST_VALUE"] = "~/"

        class MyTask(TTask):
            # This could typically be "--label-company=disney"
            x = parameter.value(default="${ENV_TEST_VALUE}some")

        target = MyTask()
        assert target.x == os.path.expanduser("~/some")

    def test_params_default_none(self):
        class TDefaultNone(TTask):
            p_str = parameter(default=None)[str]
            p_str_optional = parameter(default=None)[Optional[str]]

        target_task = TDefaultNone()
        assert target_task.p_str is None
        assert target_task.p_str_optional is None

    def test_custom_objects_dict(self):
        @task
        def t_return_custom_dict():
            # type:()->Dict
            return {1: _CustomType()}

        @task
        def t_read_custom_dict(d):
            # type:(Dict[str,str])->str
            return "ok"

        @band
        def t_band():
            a = t_return_custom_dict()
            return t_read_custom_dict(a)

        t_band.dbnd_run()
