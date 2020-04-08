from __future__ import print_function

import logging
import os

from distutils.dir_util import copy_tree

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.plugin.dbnd_plugins import is_web_enabled
from dbnd._vendor import click


logger = logging.getLogger(__name__)


@click.command()
@click.option("--no-init-db", is_flag=True)
@click.option("--overwrite", is_flag=True)
@click.option("--dbnd-home", envvar="DBND_HOME", show_envvar=True)
@click.option("--dbnd-system", envvar="DBND_SYSTEM", show_envvar=True)
@click.pass_context
def project_init(ctx, no_init_db, overwrite, dbnd_home, dbnd_system):
    """Initialize the project structure and local db"""

    from dbnd._core.errors import DatabandSystemError
    from dbnd import databand_lib_path

    os.environ["SKIP_DAGS_PARSING"] = "True"  # Exclude airflow dag examples

    conf_folder = databand_lib_path("conf/project_init")
    project_name = os.path.basename(dbnd_home)
    output_dir = os.path.dirname(dbnd_home)

    if os.path.exists(os.path.join(dbnd_home, "project.cfg")):
        if not overwrite:
            raise DatabandSystemError(
                "You are trying to re-initialize your project. You already have dbnd configuration at %s. "
                "You can force project-init by providing --overwrite flag. "
                "If you need to create/update database use `dbnd db init` instead"
                % dbnd_system
            )

        logger.warning(
            "You are re-initializing your project, all files at %s are going to be over written!"
            % dbnd_home
        )

    copy_tree(conf_folder, dbnd_home)
    click.echo("Databand project has been initialized at %s" % dbnd_home)
    config.load_system_configs(force=True)
    if no_init_db:
        click.echo("Don't forget to run `dbnd db init` ")
        return

    if is_web_enabled():
        from dbnd_web.cli.cmd_db import init as db_init

        ctx.invoke(db_init)
