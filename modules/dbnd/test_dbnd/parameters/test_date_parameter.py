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

from databand import parameters
from databand.parameters import MonthParameter, TimeDeltaParameter, YearParameter
from dbnd import parameter
from dbnd._core.utils.timezone import utc
from dbnd.testing.helpers import build_task
from dbnd.testing.orchestration_utils import TTask


class DateTask(TTask):
    day = parameter[datetime.date]


class DateHourTask(TTask):
    dh = parameters.DateHourParameter()


class DateMinuteTask(TTask):
    dm = parameters.DateMinuteParameter()


class DateSecondTask(TTask):
    ds = parameters.DateSecondParameter()


class MonthTask(TTask):
    month = MonthParameter()


class YearTask(TTask):
    year = YearParameter()


class TestDateParameter(object):
    def test_parse(self):
        d = parameter[datetime.date]._p.parse_from_str("2015-04-03")
        assert d == datetime.date(2015, 4, 3)

    def test_serialize(self):
        d = parameter[datetime.date]._p.to_str(datetime.date(2015, 4, 3))
        assert d == "2015-04-03"

    def test_parse_interface(self):
        task = build_task("DateTask", day="2015-04-03")
        assert task.day == datetime.date(2015, 4, 3)


class TestDateHourParameter(object):
    def test_parse(self):
        dh = parameters.DateHourParameter()._p.parse_from_str("2013-02-01T18")
        assert dh == datetime.datetime(2013, 2, 1, 18, 0, 0, tzinfo=utc)

    def test_date_to_dh(self):
        date = parameters.DateHourParameter()._p.normalize(datetime.date(2000, 1, 1))
        assert date == datetime.datetime(2000, 1, 1, 0, tzinfo=utc)

    def test_serialize(self):
        dh = parameters.DateHourParameter()._p.to_str(
            datetime.datetime(2013, 2, 1, 18, 0, 0)
        )
        assert dh == "2013-02-01T18"

    def test_parse_interface(self):
        task = build_task("DateHourTask", dh="2013-02-01T18")
        assert task.dh == datetime.datetime(2013, 2, 1, 18, 0, 0, tzinfo=utc)


class TestDateMinuteParameter(object):
    def test_parse(self):
        dm = parameters.DateMinuteParameter()._p.parse_from_str("2013-02-01T1842")
        assert dm == datetime.datetime(2013, 2, 1, 18, 42, 0, tzinfo=utc)

    def test_parse_padding_zero(self):
        dm = parameters.DateMinuteParameter()._p.parse_from_str("2013-02-01T1807")
        assert dm == datetime.datetime(2013, 2, 1, 18, 7, 0, tzinfo=utc)

    def test_serialize(self):
        dm = parameters.DateMinuteParameter()._p.to_str(
            datetime.datetime(2013, 2, 1, 18, 42, 0)
        )
        assert dm == "2013-02-01T1842"

    def test_serialize_padding_zero(self):
        dm = parameters.DateMinuteParameter()._p.to_str(
            datetime.datetime(2013, 2, 1, 18, 7, 0)
        )
        assert dm == "2013-02-01T1807"

    def test_parse_interface(self):
        task = build_task("DateMinuteTask", dm="2013-02-01T1842")
        assert task.dm == datetime.datetime(2013, 2, 1, 18, 42, 0, tzinfo=utc)


class TestDateSecondParameter(object):
    def test_parse(self):
        ds = parameters.DateSecondParameter()._p.parse_from_str("2013-02-01T184227")
        assert ds == datetime.datetime(2013, 2, 1, 18, 42, 27, tzinfo=utc)

    def test_serialize(self):
        ds = parameters.DateSecondParameter()._p.to_str(
            datetime.datetime(2013, 2, 1, 18, 42, 27)
        )
        assert ds == "2013-02-01T184227"

    def test_parse_interface(self):
        task = build_task("DateSecondTask", ds="2013-02-01T184227")
        assert task.ds == datetime.datetime(2013, 2, 1, 18, 42, 27, tzinfo=utc)


class TestMonthParameter(object):
    def test_parse(self):
        m = MonthParameter()._p.parse_from_str("2015-04")
        assert m == datetime.date(2015, 4, 1)

    def test_serialize(self):
        m = MonthParameter()._p.to_str(datetime.date(2015, 4, 3))
        assert m == "2015-04"

    def test_parse_interface(self):
        task = build_task("MonthTask", month="2015-04")
        assert task.month == datetime.date(2015, 4, 1)


class TestYearParameter(object):
    def test_parse(self):
        year = YearParameter()._p.parse_from_str("2015")
        assert year == datetime.date(2015, 1, 1)

    def test_serialize(self):
        year = YearParameter()._p.to_str(datetime.date(2015, 4, 3))
        assert year == "2015"

    def test_parse_interface(self):
        task = build_task("YearTask", year="2015")
        assert task.year == datetime.date(2015, 1, 1)


class TestSerializeDateParameters(object):
    def testSerialize(self):
        date = datetime.date(2013, 2, 3)
        assert parameter[datetime.date]._p.to_str(date) == "2013-02-03"
        assert YearParameter()._p.to_str(date) == "2013"
        assert MonthParameter()._p.to_str(date) == "2013-02"
        dt = datetime.datetime(2013, 2, 3, 4, 5)
        assert parameters.DateHourParameter()._p.to_str(dt) == "2013-02-03T04"


class TestSerializeTimeDeltaParameters(object):
    def testSerialize(self):
        tdelta = datetime.timedelta(weeks=5, days=4, hours=3, minutes=2, seconds=1)
        assert TimeDeltaParameter()._p.to_str(tdelta) == "5w 4d 3h 2m 1s"
        tdelta = datetime.timedelta(seconds=0)
        assert TimeDeltaParameter()._p.to_str(tdelta) == "0d"
