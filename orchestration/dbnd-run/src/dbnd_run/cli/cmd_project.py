# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import logging
import os

from distutils.dir_util import copy_tree

from dbnd._core.configuration.environ_config import get_dbnd_project_config
from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd._vendor import click
from dbnd_run.utils.dbnd_run_module import get_dbnd_run_conf_file


logger = logging.getLogger(__name__)


@click.command()
@click.option("--overwrite", is_flag=True)
@click.option("--dbnd-home", envvar="DBND_HOME", show_envvar=True)
@click.option("--dbnd-system", envvar="DBND_SYSTEM", show_envvar=True)
@click.pass_context
def project_init(ctx, overwrite, dbnd_home, dbnd_system):
    """Initialize the project structure"""
    dbnd_bootstrap(enable_dbnd_run=True)

    from dbnd._core.errors import DatabandSystemError

    if not dbnd_home:
        dbnd_home = get_dbnd_project_config().dbnd_home()

    os.environ["SKIP_DAGS_PARSING"] = "True"  # Exclude airflow dag examples

    conf_folder = get_dbnd_run_conf_file("project_init")

    if os.path.exists(os.path.join(dbnd_home, "project.cfg")):
        if not overwrite:
            raise DatabandSystemError(
                "You are trying to re-initialize your project. You already have dbnd configuration at %s. "
                "You can force project-init by providing --overwrite flag. "
                "If you need to create/update database use `dbnd-web db init` instead"
                % dbnd_system
            )

        logger.warning(
            "You are re-initializing your project, all files at %s are going to be over written!"
            % dbnd_home
        )

    copy_tree(conf_folder, dbnd_home)
    click.echo("Databand project has been initialized at %s" % dbnd_home)
    return
