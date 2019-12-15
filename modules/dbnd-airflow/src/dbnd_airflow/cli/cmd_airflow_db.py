from __future__ import print_function

import getpass
import logging
import random
import string

from argparse import Namespace

from dbnd._core.cli.utils import with_fast_dbnd_context
from dbnd._core.context.dbnd_project_env import _SHELL_COMPLETION
from dbnd._core.utils.cli import NotRequiredIf
from dbnd._vendor import click


if not _SHELL_COMPLETION:
    from airflow.bin import cli as af_cli

    from dbnd._core.errors import DatabandSystemError
    from dbnd._core.utils.project.project_fs import project_path
    from dbnd_airflow.airflow_override import patch_models, unpatch_models
    from dbnd_airflow.web.airflow_app import cached_appbuilder


logger = logging.getLogger(__name__)


@click.command()
@click.option("--no-default-user", is_flag=True, help="Do not create default user")
@with_fast_dbnd_context
def airflow_db_init(no_default_user):
    """Initialize Airflow database"""

    from airflow import settings

    unpatch_models()

    settings.DAGS_FOLDER = project_path("non_existing_dir")
    af_cli.initdb(no_default_user)
    logger.info("Finished to run airflow upgrade!")

    patch_models()

    if not no_default_user:
        create_default_user()


@click.command()
@with_fast_dbnd_context
def airflow_db_upgrade():
    """Upgrade the Airflow database to latest version"""
    unpatch_models()

    af_cli.upgradedb(Namespace())

    patch_models()


@click.command()
@click.confirmation_option(
    prompt="This will drop existing tables if they exist. Proceed?",
    help="Confirm dropping existing tables if they exist",
)
@click.option("--no-default-user", is_flag=True)
@with_fast_dbnd_context
def airflow_db_reset(no_default_user):
    """Destroy and rebuild Airflow database"""
    unpatch_models()

    af_cli.resetdb(Namespace(yes=True))

    patch_models()
    if not no_default_user:
        create_default_user()


def validate_username(ctx, param, string):
    appbuilder = cached_appbuilder()
    if appbuilder.sm.find_user(username=string):
        raise click.BadParameter("User already exist in the db")

    return string


def validate_email(ctx, param, string):
    appbuilder = cached_appbuilder()
    if appbuilder.sm.find_user(email=string):
        raise click.BadParameter("Email already exist in the db")

    return string


@click.command()
@click.option("--role", "-r", required=True)
@click.option("--username", "-u", required=True, callback=validate_username)
@click.option("--email", "-e", required=True, callback=validate_email)
@click.option("--firstname", "-f", required=True)
@click.option("--lastname", "-l", required=True)
@click.option(
    "--password",
    "-p",
    prompt=True,
    confirmation_prompt=True,
    hide_input=True,
    cls=NotRequiredIf,
    not_required_if="use_random_password",
)
@click.option("--use-random-password", is_flag=True)
@with_fast_dbnd_context
def db_user_create(**kwargs):
    """Create a new databand and web server user"""
    _db_user_create(**kwargs)


# extracted so it can be called by other functions (not from click)
def _db_user_create(
    role,
    username,
    email,
    firstname,
    lastname,
    password,
    hashed_password="",
    use_random_password=False,
):
    if use_random_password:
        password = "".join(random.choice(string.printable) for _ in range(16))

    appbuilder = cached_appbuilder()

    role = appbuilder.sm.find_role(role)
    if not role:
        raise DatabandSystemError("{} is not a valid role.".format(string))

    user = appbuilder.sm.add_user(
        username=username,
        first_name=firstname,
        last_name=lastname,
        email=email,
        role=role,
        password=password,
        hashed_password=hashed_password,
    )

    if not user:
        raise SystemExit("Failed to create user.")

    print("{} user {} with password {} created.".format(role, username, password))


def create_default_user():
    appbuilder = cached_appbuilder()
    if not appbuilder.sm.count_users():
        _db_user_create(
            role="Admin",
            username="databand",
            password="databand",
            email="support@databand.ai",
            firstname=getpass.getuser(),
            lastname="databand",
            use_random_password=False,
        )

        logger.warning(
            "------\n\nDefault user 'databand' with password 'databand' has been created. \n\n------\n"
        )

    logger.info(
        "You can create extra users using "
        "`dbnd db user-create -r Admin -u USER -e EMAIL -f FIRST_NAME -l LAST_NAME -p PASSWORD`"
    )
