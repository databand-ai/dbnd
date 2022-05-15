from tzlocal import get_localzone

from dbnd._core.utils.timezone import make_aware
from dbnd._vendor import click


class TZAwareDateTime(click.DateTime):
    name = "tzawaredatetime"

    def _try_to_convert_date(self, value, format):
        dt = super(TZAwareDateTime, self)._try_to_convert_date(value, format)
        if dt:
            return make_aware(dt, get_localzone())

    def __repr__(self):
        return "TZAwareDateTime"
