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


import datetime

import pytest

from dbnd import parameter
from targets.values import DateIntervalValueType


DI = parameter.type(DateIntervalValueType)


class TestDateInterval(object):
    def parse(self, date_str):
        return DI()._p.parse_from_str(date_str)

    def test_date(self):
        di = self.parse("2012-01-01")
        assert di.dates() == [datetime.date(2012, 1, 1)]
        assert di.next().dates() == [datetime.date(2012, 1, 2)]
        assert di.prev().dates() == [datetime.date(2011, 12, 31)]
        assert str(di) == "2012-01-01"

    def test_month(self):
        di = self.parse("2012-01")
        assert di.dates() == [
            datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in range(31)
        ]
        assert di.next().dates() == [
            datetime.date(2012, 2, 1) + datetime.timedelta(i) for i in range(29)
        ]
        assert di.prev().dates() == [
            datetime.date(2011, 12, 1) + datetime.timedelta(i) for i in range(31)
        ]
        assert str(di) == "2012-01"

    def test_year(self):
        di = self.parse("2012")
        assert di.dates() == [
            datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in range(366)
        ]
        assert di.next().dates() == [
            datetime.date(2013, 1, 1) + datetime.timedelta(i) for i in range(365)
        ]
        assert di.prev().dates() == [
            datetime.date(2011, 1, 1) + datetime.timedelta(i) for i in range(365)
        ]
        assert str(di) == "2012"

    def test_week(self):
        # >>> datetime.date(2012, 1, 1).isocalendar()
        # (2011, 52, 7)
        # >>> datetime.date(2012, 12, 31).isocalendar()
        # (2013, 1, 1)

        di = self.parse("2011-W52")
        assert di.dates() == [
            datetime.date(2011, 12, 26) + datetime.timedelta(i) for i in range(7)
        ]
        assert di.next().dates() == [
            datetime.date(2012, 1, 2) + datetime.timedelta(i) for i in range(7)
        ]
        assert str(di) == "2011-W52"

        di = self.parse("2013-W01")
        assert di.dates() == [
            datetime.date(2012, 12, 31) + datetime.timedelta(i) for i in range(7)
        ]
        assert di.prev().dates() == [
            datetime.date(2012, 12, 24) + datetime.timedelta(i) for i in range(7)
        ]
        assert str(di) == "2013-W01"

    def test_interval(self):
        di = self.parse("2012-01-01-2012-02-01")
        assert di.dates() == [
            datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in range(31)
        ]
        pytest.raises(NotImplementedError, di.next)
        pytest.raises(NotImplementedError, di.prev)
        assert di.to_string() == "2012-01-01-2012-02-01"

    def test_exception(self):
        pytest.raises(ValueError, self.parse, "xyz")

    def test_comparison(self):
        a = self.parse("2011")
        b = self.parse("2013")
        c = self.parse("2012")
        assert a < b
        assert a < c
        assert b > c
        d = self.parse("2012")
        assert d == c
        assert d == min(c, b)
        assert 3 == len({a, b, c, d})

    def test_comparison_different_types(self):
        x = self.parse("2012")
        y = self.parse("2012-01-01-2013-01-01")
        pytest.raises(TypeError, lambda: x == y)

    # def test_parameter_parse_and_default(self):
    #     month = databand.date_interval.Month(2012, 11)
    #     other = databand.date_interval.Month(2012, 10)
    #
    #     class MyTask(dbnd.Task):
    #         di = DI(default=month)
    #
    #     class MyTaskNoDefault(dbnd.Task):
    #         di = self._get_target()
    #
    #     assert MyTask().di, month)
    #     in_parse(["MyTask", "--di", "2012-10"],
    #              lambda task: assert task.di, other))
    #     task = MyTask(month)
    #     assert task.di, month)
    #     task = MyTask(di=month)
    #     assert task.di, month)
    #     task = MyTask(other)
    #     self.assertNotEqual(task.di, month)
    #
    #     def fail1():
    #         return MyTaskNoDefault()
    #
    #     self.assertRaises(databand.parameter.MissingParameterError, fail1)
    #
    #     in_parse(["MyTaskNoDefault", "--di", "2012-10"],
    #              lambda task: assert task.di, other))

    def test_hours(self):
        d = self.parse("2015")
        assert len(list(d.hours())) == 24 * 365

    def test_cmp(self):
        operators = [
            lambda x, y: x == y,
            lambda x, y: x != y,
            lambda x, y: x < y,
            lambda x, y: x > y,
            lambda x, y: x <= y,
            lambda x, y: x >= y,
        ]

        dates = [
            (1, 30, self.parse("2015-01-01-2015-01-30")),
            (1, 15, self.parse("2015-01-01-2015-01-15")),
            (10, 20, self.parse("2015-01-10-2015-01-20")),
            (20, 30, self.parse("2015-01-20-2015-01-30")),
        ]

        for from_a, to_a, di_a in dates:
            for from_b, to_b, di_b in dates:
                for op in operators:
                    assert op((from_a, to_a), (from_b, to_b)) == op(di_a, di_b)
