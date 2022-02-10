from dbnd_airflow.constants import AIRFLOW_ABOVE_13


def patch_cli_factory_subparsers(func, help_msg, args):
    from airflow.bin.cli import CLIFactory

    f = {"func": func, "help": help_msg, "args": args}
    if AIRFLOW_ABOVE_13:
        CLIFactory.deprecated_subparsers += (f,)
    else:
        CLIFactory.subparsers_dict[func.__name__] = f

    return CLIFactory
