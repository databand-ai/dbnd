from typing import Any, Optional

import attr


@attr.s(slots=True)
class ConfigValue(object):
    """
    Represent config value in config
    """

    value = attr.ib()  # type: Any

    # only value is important, everything else should be cmd=False
    source = attr.ib(cmp=False)  # type: Optional[str]
    require_parse = attr.ib(default=True, repr=False, cmp=False)  # type: bool
    override = attr.ib(default=False, repr=False, cmp=False)  # type: bool
    set_if_not_exists_only = attr.ib(default=False, repr=False, cmp=False)  # type: bool

    deprecate_message = attr.ib(default=None, repr=False, cmp=False)
    warnings = attr.ib(factory=list, repr=False, cmp=False)
