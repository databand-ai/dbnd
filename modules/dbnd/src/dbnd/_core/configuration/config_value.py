from typing import Any, Optional

import attr


@attr.s(slots=True)
class ConfigValue(object):
    """
    Represent config value in config
    """

    value = attr.ib()  # type: Any

    # only value is important, everything else should be cmd=False
    # source = attr.ib(eq=False)  # type: Optional[str]
    # require_parse = attr.ib(default=True, repr=False, eq=False)  # type: bool
    # override = attr.ib(default=False, repr=False, eq=False)  # type: bool
    # set_if_not_exists_only = attr.ib(default=False, repr=False, eq=False)  # type: bool
    #
    # deprecate_message = attr.ib(default=None, repr=False, eq=False)
    # warnings = attr.ib(factory=list, repr=False, eq=False)

    # Airflow monitor does not support 'eq' in attr.ib()
    source = attr.ib(default=False)  # type: Optional[str]
    require_parse = attr.ib(default=True)  # type: bool
    override = attr.ib(default=False)  # type: bool
    set_if_not_exists_only = attr.ib(default=False)  # type: bool

    deprecate_message = attr.ib(default=None)
    warnings = attr.ib(factory=list)
