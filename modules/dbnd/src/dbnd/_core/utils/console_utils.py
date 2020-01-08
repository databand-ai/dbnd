from dbnd._vendor.termcolor import colored


ERROR_SEPARATOR = colored("--------------------------------------", color="red")
ERROR_SEPARATOR_SMALL = colored("- - -", on_color="on_red")


def bold(value):
    return colored(value, attrs=["bold"])
