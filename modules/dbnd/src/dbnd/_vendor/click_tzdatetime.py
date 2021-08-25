from tzlocal import get_localzone

from dbnd._core.utils.timezone import make_aware
from dbnd._vendor import click


class TZAwareDateTime(click.DateTime):
    """The TZAwareDateTime type converts date strings into `datetime` objects.

    The format strings which are checked are configurable, but default to some
    common (TIMEZONE AWARE) ISO 8601 formats.

    When specifying *DateTime* formats, you should only pass a list or a tuple.
    Other iterables, like generators, may lead to surprising results.

    The format strings are processed using ``datetime.strptime``, and this
    consequently defines the format strings which are allowed.

    Parsing is tried using each format, in order, and the first format which
    parses successfully is used.

    :param formats: A list or tuple of date format strings, in the order in
                    which they should be tried. Defaults to
                    ``'%Y-%m-%d'``, ``'%Y-%m-%dT%H:%M:%S'``,
                    ``'%Y-%m-%d %H:%M:%S'``.
    """
    name = 'tzawaredatetime'

    def _try_to_convert_date(self, value, format):
        dt = super(TZAwareDateTime, self)._try_to_convert_date(value, format)
        if dt:
            return make_aware(dt, get_localzone())

    def __repr__(self):
        return 'TZAwareDateTime'
