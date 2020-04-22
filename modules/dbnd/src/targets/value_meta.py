from typing import Any, Dict, List, Optional

import attr


# keep it below VALUE_PREVIEW_MAX_LEN at web
_DEFAULT_VALUE_PREVIEW_MAX_LEN = 10000


@attr.s(slots=True)
class ValueMeta(object):
    value_preview = attr.ib()  # type: str
    data_dimensions = attr.ib()  # type: Optional[List[int]]
    data_schema = attr.ib()  # type: Optional[Dict[str,Any]]
    data_hash = attr.ib()  # type: Optional[str]


@attr.s
class ValueMetaConf(object):
    log_preview = attr.ib(default=True)  # type: bool
    log_preview_size = attr.ib(default=None)  # type: Optional[int]
    log_schema = attr.ib(default=True)  # type: bool
    log_size = attr.ib(default=True)  # type: bool

    def get_preview_size(self):
        return self.log_preview_size or _DEFAULT_VALUE_PREVIEW_MAX_LEN
