import logging

from dbnd._core.utils.string_utils import strip_whitespace
from dbnd._vendor import click
from dbnd._vendor.click.utils import make_default_short_help


logger = logging.getLogger(__name__)


class ConfigValueType(click.ParamType):
    def __init__(self):
        self.name = "config"

    def convert(self, value, param, ctx):
        from targets.values.structure import _PARSABLE_PARAM_PREFIX
        from dbnd._core.utils import json_utils

        value = value.strip()
        # support for --set '{ "a":2, "b":33}'
        if value[0] in _PARSABLE_PARAM_PREFIX:
            return json_utils.loads(value)

        name, sep, var = value.partition("=")
        return {name: var}


def _help(string):
    if not string:
        return "-"
    return strip_whitespace(string)


def _help_short(string):
    return make_default_short_help(string or "-")
