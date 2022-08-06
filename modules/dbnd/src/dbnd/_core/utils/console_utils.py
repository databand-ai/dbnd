# © Copyright Databand.ai, an IBM Company 2022

from dbnd._vendor.termcolor import colored


# values should be calculated at run time - so colored function could be removed if needed
def error_separator():
    return colored("--------------------------------------", color="red")


def error_separator_small():
    return colored("- - -", on_color="on_red")


def bold(value):
    return colored(value, attrs=["bold"])


def underline(value):
    return colored(value, attrs=["underline"])


def red(value):
    return colored(value, color="red")


def cyan(value):
    return colored(value, color="cyan")
