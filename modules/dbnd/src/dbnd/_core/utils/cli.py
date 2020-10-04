from functools import reduce, wraps
from operator import xor

from dbnd._vendor import click


class NotRequiredIf(click.Option):
    def __init__(self, *args, **kwargs):
        self.not_required_if = kwargs.pop("not_required_if")
        assert self.not_required_if, "'not_required_if' parameter required"
        kwargs["help"] = (
            kwargs.get("help", "")
            + " NOTE: This argument is mutually exclusive with %s"
            % self.not_required_if
        ).strip()
        super(NotRequiredIf, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        we_are_present = self.name in opts
        other_present = self.not_required_if in opts

        if other_present:
            if we_are_present:
                raise click.UsageError(
                    "Illegal usage: `%s` is mutually exclusive with `%s`"
                    % (self.name, self.not_required_if)
                )
            else:
                self.prompt = None

        return super(NotRequiredIf, self).handle_parse_result(ctx, opts, args)


def only_one(*args):
    return reduce(xor, map(bool, args))


def requierd_mutually_exclude_options(*options):
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

            if any(relevant_options) and kwargs.get(root) is None:
                raise click.UsageError(
                    "Illegal usage: `in oprder to use any of this options {options}, "
                    "the {root} option must be set".format(
                        root=root, options=dependents
                    )
                )

            func(**kwargs)

        return inner

    return decorator
