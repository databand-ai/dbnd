import abc
import datetime

import pytz

from targets.values import DateTimeValueType, DateValueType


_UNIX_EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)


def _add_months(date, months):
    """
    Add ``months`` months to ``date``.

    Unfortunately we can't use timedeltas to add months because timedelta counts in days
    and there's no foolproof way to add N months in days without counting the number of
    days per month.
    """
    year = date.year + (date.month + months - 1) // 12
    month = (date.month + months - 1) % 12 + 1
    return datetime.date(year=year, month=month, day=1)


class _DateCustom(DateValueType):
    """
    Base class ValueType for date (not datetime).
    """

    def __init__(self, interval=1, start=None, **kwargs):
        super(_DateCustom, self).__init__(**kwargs)
        self.interval = interval
        self.start = start if start is not None else _UNIX_EPOCH.date()

    @property
    @abc.abstractmethod
    def date_format(self):
        """
        Override me with a :py:meth:`~datetime.date.strftime` string.
        """
        pass

    def to_str(self, dt):
        """
        Converts the date to a string using the :py:attr:`~_DateValueTypeBase.date_format`.
        """
        return dt.strftime(self.date_format)

    def parse_from_str(self, x):
        return datetime.datetime.strptime(x, self.date_format).date()


class MonthValueType(_DateCustom):
    """
    ValueType whose value is a :py:class:`~datetime.date`, specified to the month
    (day of :py:class:`~datetime.date` is "rounded" to first of the month).

    A MonthValueType is a Date string formatted ``YYYY-MM``. For example, ``2013-07`` specifies
    July of 2013.
    """

    date_format = "%Y-%m"

    def _add_months(self, date, months):
        """
        Add ``months`` months to ``date``.

        Unfortunately we can't use timedeltas to add months because timedelta counts in days
        and there's no foolproof way to add N months in days without counting the number of
        days per month.
        """
        year = date.year + (date.month + months - 1) // 12
        month = (date.month + months - 1) % 12 + 1
        return datetime.date(year=year, month=month, day=1)

    def next_in_enumeration(self, value):
        return self._add_months(value, self.interval)

    def normalize(self, value):
        value = super(MonthValueType, self).normalize(value)

        months_since_start = (value.year - self.start.year) * 12 + (
            value.month - self.start.month
        )
        months_since_start -= months_since_start % self.interval

        return self._add_months(self.start, months_since_start)


class YearValueType(_DateCustom):
    """
    ValueType whose value is a :py:class:`~datetime.date`, specified to the year
    (day and month of :py:class:`~datetime.date` is "rounded" to first day of the year).

    A YearValueType is a Date string formatted ``YYYY``.
    """

    date_format = "%Y"

    def next_in_enumeration(self, value):
        return value.replace(year=value.year + self.interval)

    def normalize(self, value):
        value = super(YearValueType, self).normalize(value)

        delta = (value.year - self.start.year) % self.interval
        return datetime.date(year=value.year - delta, month=1, day=1)


class _DatetimeCustom(DateTimeValueType):
    """
    Base class ValueType for datetime
    """

    def __init__(self, interval=1, start=None):
        super(_DatetimeCustom, self).__init__()
        self.interval = interval
        self.start = start if start is not None else _UNIX_EPOCH

    def parse_from_str(self, s):
        """
        Parses a string to a :py:class:`~datetime.datetime`.
        """
        v = datetime.datetime.strptime(s, self.date_format)
        return v.replace(tzinfo=pytz.UTC)

    @property
    @abc.abstractmethod
    def date_format(self):
        """
        Override me with a :py:meth:`~datetime.date.strftime` string.
        """
        pass

    @property
    @abc.abstractmethod
    def _timedelta(self):
        """
        How to move one interval of this type forward (i.e. not counting self.interval).
        """
        pass

    def to_str(self, dt):
        """
        Converts the date to a string using the :py:attr:`~_DatetimeValueTypeBase.date_format`.
        """
        if dt is None:
            return str(dt)
        return dt.strftime(self.date_format)

    @staticmethod
    def _convert_to_dt(dt):
        if not isinstance(dt, datetime.datetime):
            dt = datetime.datetime.combine(dt, datetime.time.min)
            dt = dt.replace(tzinfo=pytz.utc)
        return dt

    def normalize(self, dt):
        """
        Clamp dt to every Nth :py:attr:`~_DatetimeValueTypeBase.interval` starting at
        :py:attr:`~_DatetimeValueTypeBase.start`.
        """
        dt = super(_DatetimeCustom, self).normalize(dt)

        dt = dt.replace(
            microsecond=0
        )  # remove microseconds, to avoid float rounding issues.
        delta = (dt - self.start).total_seconds()
        granularity = (self._timedelta * self.interval).total_seconds()
        return dt - datetime.timedelta(seconds=delta % granularity)

    def next_in_enumeration(self, value):
        return value + self._timedelta * self.interval


class DateHourValueType(_DatetimeCustom):
    """
    ValueType whose value is a :py:class:`~datetime.datetime` specified to the hour.

    A DateHourValueType is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the hour. For example, ``2013-07-10T19`` specifies July 10, 2013 at
    19:00.
    """

    date_format = "%Y-%m-%dT%H"  # ISO 8601 is to use 'T'
    _timedelta = datetime.timedelta(hours=1)


class DateMinuteValueType(_DatetimeCustom):
    """
    ValueType whose value is a :py:class:`~datetime.datetime` specified to the minute.

    A DateMinuteValueType is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the minute. For example, ``2013-07-10T1907`` specifies July 10, 2013 at
    19:07.

    The interval parameter can be used to clamp this parameter to every N minutes, instead of every minute.
    """

    date_format = "%Y-%m-%dT%H%M"
    _timedelta = datetime.timedelta(minutes=1)


class DateSecondValueType(_DatetimeCustom):
    """
    ValueType whose value is a :py:class:`~datetime.datetime` specified to the second.

    A DateSecondValueType is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the second. For example, ``2013-07-10T190738`` specifies July 10, 2013 at
    19:07:38.

    The interval parameter can be used to clamp this parameter to every N seconds, instead of every second.
    """

    date_format = "%Y-%m-%dT%H%M%S"
    _timedelta = datetime.timedelta(seconds=1)
