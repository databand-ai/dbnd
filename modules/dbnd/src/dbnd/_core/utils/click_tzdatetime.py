# © Copyright Databand.ai, an IBM Company 2022
from datetime import datetime

from tzlocal import get_localzone

from dbnd._core.utils.timezone import is_localized, make_aware
from dbnd._vendor import click


class TZAwareDateTime(click.DateTime):
    name = "tzawaredatetime"

    def _try_to_convert_date(self, value, format):
        dt: datetime = super(TZAwareDateTime, self)._try_to_convert_date(value, format)
        if dt and not is_localized(dt):
            return make_aware(dt, get_localzone())
        return dt

    def __repr__(self):
        return "TZAwareDateTime"
