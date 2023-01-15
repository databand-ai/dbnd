# © Copyright Databand.ai, an IBM Company 2022

from dbnd_run.airflow.compat import AIRFLOW_ABOVE_13


def patch_cli_factory_subparsers(func, help_msg, args):
    from airflow.bin.cli import CLIFactory

    f = {"func": func, "help": help_msg, "args": args}
    if AIRFLOW_ABOVE_13:
        CLIFactory.deprecated_subparsers += (f,)
    else:
        CLIFactory.subparsers_dict[func.__name__] = f

    return CLIFactory
