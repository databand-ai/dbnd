from dbnd._core.cli.cmd_show import _list_tasks
from dbnd._core.configuration.dbnd_config import config as dbnd_config

# © Copyright Databand.ai, an IBM Company 2022
from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd._vendor import click
from dbnd_run.cli.service_auto_completer import completer
from dbnd_run.utils.user_code import load_user_modules


@click.command()
@click.argument("search", default="", autocompletion=completer.task())
@click.option("--module", "-m", help="Used for dynamic loading of modules")
@click.pass_context
def show_tasks(ctx, module, search):
    """Show and search tasks"""

    dbnd_bootstrap(enable_dbnd_run=True)

    load_user_modules(dbnd_config, modules=module)
    _list_tasks(ctx, search, is_config=False)
