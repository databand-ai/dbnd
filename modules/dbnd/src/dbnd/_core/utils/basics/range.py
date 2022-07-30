# © Copyright Databand.ai, an IBM Company 2022

import datetime

from datetime import timedelta
from typing import Iterable


def period_dates(target_date, period, step=timedelta(days=1)):
    # type: (datetime.date, datetime.timedelta, datetime.timedelta) -> Iterable[datetime.date]
    while period.total_seconds() > 0:
        yield target_date
        period -= step
        target_date -= step
