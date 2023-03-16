# © Copyright Databand.ai, an IBM Company 2022

import logging
import re

from collections import OrderedDict
from typing import Any, Dict

from pythonjsonlogger import jsonlogger


logger = logging.getLogger(__name__)


def extract_key_value_pairs(msg: Any, args: Any) -> Dict[str, str]:
    if not isinstance(msg, str) or "=" not in msg:
        return {}

    # the simpler solution of extracting key=value from already interpolated string
    # has many edge cases related to whitespaces, i.e
    # log.info("key1=%s key2=%s,key3=123","some long string", "short,str")
    # which is rendered to "key1=some long string key2=short,str,key3=123" so not easy
    # to actually catch that "some long string" is one value and "short,str" is other
    #
    # first catch all key=value and mark it, i.e
    # "key1=value la la key2=%s key3=%d" =>
    #   ">>>key1=value<<< la la >>>key2=%s<<< >>>key3=%d<<<"
    # after interpolation it will allow to catch keys with the actual values
    #   ">>>key1=value<<< la la >>>key2=some other value<<< >>>key3=123<<<"
    new_msg = re.sub(r"(\w+=%?([^=,;\s]*\w))", ">>>\\1<<<", msg)
    if args:
        # interpolate only if args not empty: same as logging.LogRecord.getMessage
        new_msg = new_msg % args
    # extract all >>>key=value<<< pairs and update log record accordingly
    extracted_pairs = {
        m["key"]: m["value"]
        for m in re.finditer(r">>>(?P<key>\w+)=(?P<value>.*?)<<<", new_msg)
    }
    return extracted_pairs


class JsonFormatter(jsonlogger.JsonFormatter):
    always_log_attrs = {
        "funcName",
        "module",
        "levelname",
        "name",
        "process",
        "threadName",
    }
    format_parse_exception_message = "Exception during key=value extraction"

    def __init__(self, *args, **kwargs):
        kwargs = dict(
            rename_fields={"levelname": "severity", "name": "logger"},  # rename for GCP
            timestamp="ts",
            **kwargs,
        )
        super().__init__(*args, **kwargs)

    def parse(self):
        return self.always_log_attrs.union(super().parse())

    def format(self, record: logging.LogRecord):
        # prevent recursion
        if record.msg != self.format_parse_exception_message:
            try:
                d = extract_key_value_pairs(record.msg, record.args)
                for k, v in d.items():
                    if not hasattr(record, k):
                        setattr(record, k, v)
            except Exception:
                logger.exception(
                    self.format_parse_exception_message,
                    extra={"orig_msg": record.msg, "orig_args": record.args},
                )

        return super().format(record)

    def process_log_record(self, log_record):
        d = OrderedDict()
        # make ts / severity / message first elements for readability
        for key in ("ts", "severity", "message"):
            val = log_record.pop(key, None)
            if val:
                d[key] = val
        d.update(log_record)
        return d
