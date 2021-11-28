from typing import Any, List, Optional

import attr

from more_itertools import first


class ConfigValuePriority(object):
    """
    Define levels of priority and comparison between them
    """

    # lowest level, used to define a fallback value
    FALLBACK = 10
    # default level
    NORMAL = 50
    # overriding any previous level
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

    # mark this config value should be merged
    extend = attr.ib(default=False)  # type: bool
    deprecate_message = attr.ib(default=None)
    warnings = attr.ib(factory=list)


def override(value):
    return ConfigValue(value=value, source=None, priority=ConfigValuePriority.OVERRIDE)


def default(value):
    return ConfigValue(value=value, source=None, priority=ConfigValuePriority.FALLBACK)


def extend(value):
    if not (isinstance(value, list) or isinstance(value, dict)):
        raise ValueError(
            "extend is not supported for type {value_type}.\n help: consider using list or dict instead of {value}".format(
                value_type=type(value), value=value
            )
        )

    return ConfigValue(
        value=value, source=None, priority=ConfigValuePriority.NORMAL, extend=True,
    )


def fold_config_value(stack, lower):
    # type: (List[ConfigValue], Optional[ConfigValue]) -> List[ConfigValue]
    """
    Collect config values stack by comparing the top of the stack to the current config value.
    If matching priority - it will try to link them if the top of the stack allow, other wise will drop the lower value.
    Otherwise return a stack with the highest priority.
    """
    if lower:
        bottom = first(stack, default=None)
        if bottom is None or bottom.priority < lower.priority:
            # remember highest priority
            return [lower]

        if bottom.extend:
            return [lower] + stack

    return stack
