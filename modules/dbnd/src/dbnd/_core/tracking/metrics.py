from datetime import datetime

import attr

from dbnd._core.constants import _DbndDataClass


@attr.s
class Metric(_DbndDataClass):
    key = attr.ib()  # type: str
    value = attr.ib()
    timestamp = attr.ib()  # type: datetime


@attr.s
class Artifact(_DbndDataClass):
    path = attr.ib()  # type: str
