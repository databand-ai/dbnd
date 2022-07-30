# © Copyright Databand.ai, an IBM Company 2022

from functools import reduce, wraps
from operator import xor

from dbnd._vendor import click


def only_one(*args):
    return reduce(xor, map(bool, args))


def required_mutually_exclusive_options(*options):
    """
    Creates a decorator for click commands that make sure that
    only one, and at least one, of `options` is set while calling the click command.
    It also set a relevant help message for the mutually excluded options.
    """

    def decorator(func):
        options_obj = [
            option for option in func.__click_params__ if option.name in options
        ]
        options_names = {option.name for option in options_obj}
        for option in options_obj:
            current_help = "" if option.help is None else option.help + " "
            others = list(options_names - {option.name})
            added_msg = "**mutually excluded with {others}".format(others=others)
            option.help = current_help + added_msg

        @wraps(func)
        def inner(**kwargs):
            relevant_options = [
                value for name, value in kwargs.items() if name in options
            ]
            if not only_one(*relevant_options):
                raise click.UsageError(
                    "Illegal usage: `at least one, and only one of the options [{options}] is required` ".format(
                        options=",".join("--" + op_name for op_name in options)
                    )
                )
            func(**kwargs)

        return inner

    return decorator


def options_dependency(root, *dependents):
    """
    Creates a decorator for click commands that make sure that only when `root`
    option is set all the others `dependents` could be used.
    It also set a relevant help message for the dependents options.
    """

    def decorator(func):
        options_obj = [
            option for option in func.__click_params__ if option.name in dependents
        ]
        for option in options_obj:
            current_help = "" if option.help is None else option.help + " "
            added_msg = "**depend on {root} option".format(root=root)
            option.help = current_help + added_msg

        @wraps(func)
        def inner(**kwargs):
            relevant_options = [
                value for name, value in kwargs.items() if name in dependents
            ]
            if any(relevant_options) and not kwargs.get(root):
                raise click.UsageError(
                    "Illegal usage: In-order to use any of those options {options}, "
                    "the `{root}` option must be set".format(
                        root=root, options=dependents
                    )
                )

            func(**kwargs)

        return inner

    return decorator
