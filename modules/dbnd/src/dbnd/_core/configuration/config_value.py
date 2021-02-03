from typing import Any, Optional

import attr


class ConfigValuePriority(object):
    DEFAULT = 10
    NORMAL = 50
    OVERRIDE = 100


@attr.s(slots=True)
class ConfigValue(object):
    """
    Represent config value in config
    """

    value = attr.ib()  # type: Any

    # only value is important, everything else should be cmd=False
    # source = attr.ib(eq=False)  # type: Optional[str]
    # require_parse = attr.ib(default=True, repr=False, eq=False)  # type: bool
    #
    # deprecate_message = attr.ib(default=None, repr=False, eq=False)
    # warnings = attr.ib(factory=list, repr=False, eq=False)

    # Airflow monitor does not support 'eq' in attr.ib()
    source = attr.ib(default="")  # type: Optional[str]
    require_parse = attr.ib(default=True)  # type: bool
    priority = attr.ib(default=ConfigValuePriority.NORMAL)  # type: int

    deprecate_message = attr.ib(default=None)
    warnings = attr.ib(factory=list)


def override(value):
    return ConfigValue(value=value, source=None, priority=ConfigValuePriority.OVERRIDE)


def default(value):
    return ConfigValue(value=value, source=None, priority=ConfigValuePriority.DEFAULT)
