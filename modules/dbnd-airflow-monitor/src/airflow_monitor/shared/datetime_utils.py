# © Copyright Databand.ai, an IBM Company 2024

from datetime import datetime, timezone

import dateutil.parser


@staticmethod
def format_datetime(datetime_obj: datetime, date_format: str) -> str:
    return datetime_obj.strftime(date_format)


@staticmethod
def parse_datetime(date_str: str, date_format: str) -> datetime:
    return datetime.strptime(date_str, date_format).replace(tzinfo=timezone.utc)


@staticmethod
def iso_parse_datetime(date_str: str) -> datetime:
    return dateutil.parser.isoparse(date_str)


@staticmethod
def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)
