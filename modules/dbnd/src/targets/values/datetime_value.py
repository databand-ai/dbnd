# © Copyright Databand.ai, an IBM Company 2022

import datetime

import pytz

from dbnd._core.utils.timezone import utcnow
from dbnd._vendor import pendulum
from targets.values import ValueType


class DateAlias(object):
    now = "now"
    today = "today"
    yesterday = "yesterday"


class DateValueType(ValueType):
    """
    ValueType whose value is a :py:class:`~datetime.date`.

    A DateValueType is a Date string formatted ``YYYY-MM-DD``. For example, ``2013-07-10`` specifies
    July 10, 2013.

    DateValueTypes are 90% of the time used to be interpolated into file system paths or the like.
    Here is a gentle reminder of how to interpolate date parameters into strings:

    .. code:: python

        class MyTask(dbnd.Task):
            date = databand.DateValueType()

            def run(self):
                templated_path = "/my/path/to/my/dataset/{date:%Y/%m/%d}/"
                instantiated_path = templated_path.format(date=self.date)
                # print(instantiated_path) --> /my/path/to/my/dataset/2016/06/09/
                # ... use instantiated_path ...

    To set this parameter to default to the current day. You can write code like this:

    .. code:: python

        import datetime

        class MyTask(dbnd.Task):
            date = databand.DateValueType(default=datetime.date.today())
    """

    type = datetime.date
    date_format = "%Y-%m-%d"

    def next_in_enumeration(self, value):
        return value + datetime.timedelta(days=1)

    def normalize(self, value):
        if value is None:
            return None

        if isinstance(value, datetime.datetime):
            value = value.date()
        return value

    def parse_from_str(self, s):
        """
        Parses a date string formatted like ``YYYY-MM-DD``.
        """
        if s.lower() == DateAlias.today:
            return utcnow().date()
        elif s.lower() == DateAlias.yesterday:
            return utcnow().date() - datetime.timedelta(days=1)
        else:
            return datetime.datetime.strptime(s, self.date_format).date()

    def to_str(self, dt):
        """
        Converts the date to a string using the :py:attr:`~_DateValueTypeBase.date_format`.
        """
        if dt is None:
            return str(dt)
        return dt.strftime(self.date_format)


class DateTimeValueType(ValueType):
    """
    DateTimeValueType whose value is a :py:class:`~datetime.datetime` specified to the second.

    A DateSecondValueType is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the second. For example, ``2013-07-10T190738`` specifies July 10, 2013 at
    19:07:38.
    """

    type = datetime.datetime
    date_format = "%Y-%m-%dT%H%M%S.%f"

    def parse_from_str(self, s):
        """
        Parses a string to a :py:class:`~datetime.datetime`.
        """
        try:
            v = datetime.datetime.strptime(s, self.date_format)
            return v.replace(tzinfo=pytz.UTC)
        except (ValueError):
            return pendulum.parse(s, tz=pytz.UTC)

    def to_str(self, dt):
        """
        Converts the date to a string using the :py:attr:`~_DatetimeValueTypeBase.date_format`.
        """
        if dt is None:
            return str(dt)
        return dt.strftime(self.date_format)

    def normalize(self, dt):
        """
        Clamp dt to every 1th :py:attr:`~DateTimeValueType._timedelta` starting at
        :py:attr:`~_DatetimeValueTypeBase.start`.
        """
        if dt is None:
            return None
        if not isinstance(dt, datetime.datetime):
            dt = datetime.datetime.combine(dt, datetime.time.min)
            dt = dt.replace(tzinfo=pytz.utc)
        return dt
