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

import targets.values.timedelta_value

from dbnd._core.utils.timezone import utc
from targets import values as v


class TestDateValueType(object):
    def test_parse(self):
        d = v.DateValueType().parse_from_str("2015-04-03")
        assert d == datetime.date(2015, 4, 3)

    def test_dump_to_str(self):
        d = v.DateValueType().to_str(datetime.date(2015, 4, 3))
        assert d == "2015-04-03"


class TestDateTimeValueType(object):
    def test_parse(self):
        dh = v.DateTimeValueType().parse_from_str("2013-02-01T180000.00")
        assert dh == datetime.datetime(2013, 2, 1, 18, 0, 0, tzinfo=utc)

    def test_date_to_dh(self):
        date = v.DateTimeValueType().normalize(datetime.date(2000, 1, 1))
        assert date == datetime.datetime(2000, 1, 1, 0, tzinfo=utc)

    def test_dump_to_str(self):
        dh = v.DateTimeValueType().to_str(datetime.datetime(2013, 2, 1, 18, 0, 0))
        assert dh == "2013-02-01T180000.000000"


class TestSerializeDateValueTypes(object):
    def testSerialize(self):
        date = datetime.date(2013, 2, 3)
        assert v.DateValueType().to_str(date) == "2013-02-03"
        dt = datetime.datetime(2013, 2, 3, 4, 5)
        assert v.DateValueType().to_str(dt) == "2013-02-03"


class TestSerializeTimeDeltaValueTypes(object):
    def testSerialize(self):
        tdelta = datetime.timedelta(weeks=5, days=4, hours=3, minutes=2, seconds=1)
        assert (
            targets.values.timedelta_value.TimeDeltaValueType().to_str(tdelta)
            == "5w 4d 3h 2m 1s"
        )
        tdelta = datetime.timedelta(seconds=0)
        assert (
            targets.values.timedelta_value.TimeDeltaValueType().to_str(tdelta) == "0d"
        )
