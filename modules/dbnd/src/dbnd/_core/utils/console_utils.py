import re

from dbnd._vendor.termcolor import colored


# todo: those are const - should be calculate at run time (so colored function could could be removed if needed)
ERROR_SEPARATOR = colored("--------------------------------------", color="red")
ERROR_SEPARATOR_SMALL = colored("- - -", on_color="on_red")


def bold(value):
    return colored(value, attrs=["bold"])


COLOR_REGEX = re.compile(r"\x1b?\[(\d;)?\d*m")


def uncolor(text):
    """removes the ANSII color formatting from text"""
    return COLOR_REGEX.sub("", text)
