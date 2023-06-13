from dbnd._core.cli.cmd_show import _list_tasks

# © Copyright Databand.ai, an IBM Company 2022
from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd._vendor import click
from dbnd.orchestration.cli.service_auto_completer import completer


@click.command()
@click.argument("search", default="", autocompletion=completer.task())
@click.option("--module", "-m", help="Used for dynamic loading of modules")
@click.pass_context
def show_tasks(ctx, module, search):
    """Show and search tasks"""

    dbnd_bootstrap(enable_dbnd_run=True)
    _list_tasks(ctx, module, search, is_config=False)
