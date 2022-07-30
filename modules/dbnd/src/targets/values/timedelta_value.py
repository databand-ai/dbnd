# © Copyright Databand.ai, an IBM Company 2022

import datetime

import six

from dbnd._core.errors import ParameterError
from dbnd._core.utils.date_interval import DateInterval
from targets.values import ValueType


class DateIntervalValueType(ValueType):
    """
    A ValueType whose value is a :py:class:`~databand.date_interval.DateInterval`.

    Date Intervals are specified using the ISO 8601 date notation for dates
    (eg. "2015-11-04"), months (eg. "2015-05"), years (eg. "2015"), or weeks
    (eg. "2015-W35"). In addition, it also supports arbitrary date intervals
    provided as two dates separated with a dash (eg. "2015-11-04-2015-12-04").
    """

    type = DateInterval

    def parse_from_str(self, s):
        """
        Parses a :py:class:`~databand.date_interval.DateInterval` from the input.

        see :py:mod:`databand.date_interval`
          for details on the parsing of DateIntervals.
        """
        # TODO: can we use xml.utils.iso8601 or something similar?

        from dbnd._core.utils import date_interval as d

        for cls in [d.Year, d.Month, d.Week, d.Date, d.Custom]:
            i = cls.parse(s)
            if i:
                return i

        raise ValueError("Invalid date interval - could not be parsed")


class TimeDeltaValueType(ValueType):
    """
    Class that maps to timedelta using strings in any of the following forms:

     * ``n {w[eek[s]]|d[ay[s]]|h[our[s]]|m[inute[s]|s[second[s]]}`` (e.g. "1 week 2 days" or "1 h")
        Note: multiple arguments must be supplied in longest to shortest unit order
     * ISO 8601 duration ``PnDTnHnMnS`` (each field optional, years and months not supported)
     * ISO 8601 duration ``PnW``

    See https://en.wikipedia.org/wiki/ISO_8601#Durations
    """

    type = datetime.timedelta

    def parse_from_str(self, input):
        """
        Parses a time delta from the input.

        See :py:class:`TimeDeltaValueType` for details on supported formats.
        """
        result = _parseIso8601(input)
        if not result:
            result = _parseSimple(input)
        if result is not None:
            return result
        else:
            raise ParameterError("Invalid time delta - could not parse %s" % input)

    def to_str(self, x):
        """
        Converts datetime.timedelta to a string

        :param x: the value to serialize.
        """
        weeks = x.days // 7
        days = x.days % 7
        hours = x.seconds // 3600
        minutes = (x.seconds % 3600) // 60
        seconds = (x.seconds % 3600) % 60
        result = " ".join(
            [
                "%s%s" % (value, n)
                for n, value in [
                    ("w", weeks),
                    ("d", days),
                    ("h", hours),
                    ("m", minutes),
                    ("s", seconds),
                ]
                if value
            ]
        )
        if not result:
            result = "0d"
        # result = "{} w {} d {} h {} m {} s".format(weeks, days, hours, minutes, seconds)
        return result


def _apply_regex(regex, input):
    import re

    re_match = re.match(regex, input)
    if re_match and any(re_match.groups()):
        kwargs = {}
        has_val = False
        for k, v in six.iteritems(re_match.groupdict(default="0")):
            val = int(v)
            if val > -1:
                has_val = True
                kwargs[k] = val
        if has_val:
            return datetime.timedelta(**kwargs)


def _parseIso8601(input):
    def field(key):
        return r"(?P<%s>\d+)%s" % (key, key[0].upper())

    def optional_field(key):
        return "(%s)?" % field(key)

    # A little loose: ISO 8601 does not allow weeks in combination with other fields, but this regex does (as does python timedelta)
    regex = "P(%s|%s(T%s)?)" % (
        field("weeks"),
        optional_field("days"),
        "".join([optional_field(key) for key in ["hours", "minutes", "seconds"]]),
    )
    return _apply_regex(regex, input)


def _parseSimple(input):
    keys = ["weeks", "days", "hours", "minutes", "seconds"]
    # Give the digits a regex group name from the keys, then look for text with the first letter of the key,
    # optionally followed by the rest of the word, with final char (the "s") optional
    regex = "".join(
        [r"((?P<%s>\d+) ?%s(%s)?(%s)? ?)?" % (k, k[0], k[1:-1], k[-1]) for k in keys]
    )
    return _apply_regex(regex, input)
