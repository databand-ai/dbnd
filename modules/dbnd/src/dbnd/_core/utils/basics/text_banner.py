import logging
import re

import six

from six import StringIO

from dbnd._core.configuration.environ_config import ENV_DBND__NO_TABLES
from dbnd._core.utils import json_utils
from dbnd._core.utils.basics.environ_utils import environ_enabled
from dbnd._core.utils.platform import windows_compatible_mode
from dbnd._core.utils.string_utils import safe_short_string
from dbnd._core.utils.terminal import get_terminal_size
from dbnd._core.utils.traversing import traverse, traverse_to_str
from dbnd._vendor.tabulate import tabulate
from dbnd._vendor.termcolor import colored


logger = logging.getLogger(__name__)

_TAB = "\n\t      "


def safe_string(value, max_value_len=1000):
    try:
        if not isinstance(value, six.string_types):
            value = str(value)

        return safe_short_string(value=value, max_value_len=max_value_len)
    except Exception as ex:
        return "Failed to convert value to string:%s" % ex


class TextBanner(StringIO):
    def __init__(self, msg, color="white"):
        StringIO.__init__(self)

        self.color = color
        self._banner_separator = colored("==================== \n", color)
        self._banner_msg = colored("= %s\n" % msg, color)
        self._section_separator = "-------\n"
        # p = pprint.PrettyPrinter(indent=2, width=140)

        self.write("\n")
        self.write(self._banner_separator)
        self.write(self._banner_msg)

    def f_io(self, structure):

        structure_str = traverse_to_str(structure)
        structure_str = traverse(
            structure_str,
            lambda x: x
            if not x or len(x) <= 600
            else ("%s... (%s files)" % (x[:400], len(x.split(",")))),
        )
        dumped = json_utils.dumps(structure_str, indent=2)
        return dumped

    def f_struct(self, structure):
        # return p.pformat(_dump_struct(structure))
        structure_str = traverse_to_str(structure)
        dumped = json_utils.dumps(structure_str, indent=2)
        return dumped

    def f_simple_dict(self, params, skip_if_empty=False):
        return "  ".join(
            "%s=%s" % (key, value)
            for key, value in params
            if not skip_if_empty or value is not None
        )

    def column_properties(self, name, params, skip_if_empty=False, **kwargs):
        return self.column(
            name,
            self.f_simple_dict(params, skip_if_empty=skip_if_empty),
            skip_if_empty=skip_if_empty,
            **kwargs
        )

    def column(self, name, value, newline=True, raw_name=False, skip_if_empty=False):
        # if separate:
        #     s.write("  \n")
        if skip_if_empty and value is None:
            return self

        if isinstance(value, six.string_types):
            if six.PY2:
                value = value.encode("utf-8", errors="ignore")
            else:
                value = str(value)
        else:
            value = str(value)

        if skip_if_empty and not value:
            return self

        if "\n" in value:
            value = _TAB + _TAB.join(value.split("\n"))

        # remove last \n,  use new line or new section if you nee that
        if value[-1:] == "\n":
            value = value[:-1]

        self.write(
            " {name:<18} : {value}".format(
                name=name if raw_name else colored(name, attrs=["bold"]), value=value
            )
        )

        if newline:
            self.write("\n")
        return self

    def new_section(self):
        self.write(self._section_separator)

    def new_line(self):
        self.write("\n")

    def get_banner_str(self):
        self.write(colored("=\n", self.color))
        self.write(self._banner_separator)
        return self.getvalue()


def safe_tabulate(tabular_data, headers, **kwargs):
    terminal_columns, _ = get_terminal_size()
    # fancy_grid format has utf-8 characters (in corners of table)
    # cp1252 fails to encode that
    fancy_grid = not windows_compatible_mode and not environ_enabled(
        ENV_DBND__NO_TABLES
    )
    tablefmt = "fancy_grid" if fancy_grid else "grid"
    table = tabulate(tabular_data, headers=headers, tablefmt=tablefmt, **kwargs)
    if table and max(map(len, table.split())) >= terminal_columns:
        table = tabulate(tabular_data, headers=headers, tablefmt="plain", **kwargs)
    return table
