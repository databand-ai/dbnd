import logging
import os.path

from dbnd._core.utils.basics.format_exception import format_exception_as_str
from dbnd._core.utils.project.project_fs import abs_join, relative_path
from dbnd._vendor import click


logger = logging.getLogger(__name__)
_airflow_lib_home_default = relative_path(__file__)


def dbnd_airflow_path(*path):
    return abs_join(_airflow_lib_home_default, *path)


def link_dropin_file(source_path, target_path, unlink_first=True, name="file"):
    sub_dir = os.path.dirname(target_path)

    if unlink_first and os.path.islink(target_path):
        try:
            logger.info("unlinking existing %s at %s", name, target_path)
            os.unlink(target_path)
        except Exception:
            logger.error(
                "failed to unlink %s at %s: %s",
                (name, target_path, format_exception_as_str()),
            )
            return

    if not os.path.exists(target_path):
        try:
            logger.info("Linking %s to %s.", source_path, target_path)
            if not os.path.exists(sub_dir):
                os.makedirs(sub_dir)
            os.symlink(source_path, target_path)
        except Exception:
            logger.error(
                "failed to link %s in the airflow dags_folder: %s"
                % (name, format_exception_as_str())
            )


@click.command()
@click.option("--username", "-u", help="Username to add", required=True)
@click.option("--password", "-p", help="User's password", required=True)
@click.option("--email", "-e", help="User's email")
def create_user(username, password, email):
    # This import depends on flask_bcrypt
    # You should get it via installation of: 'apache-airflow[google_auth]'
    # More info: http://airflow.apache.org/docs/stable/security.html
    from airflow.contrib.auth.backends.password_auth import PasswordUser

    from airflow import models
    from airflow.settings import Session

    # Create user for experimental api users
    session = Session()
    print("sql_alchemy_conn: %s" % str(session.bind))
    try:
        user = (
            session.query(PasswordUser)
            .filter(PasswordUser.username == username)
            .one_or_none()
        )
        if not user:
            user = PasswordUser(models.User())
            user.username = username

        user.password = password
        user.email = email

        session.add(user)
        session.commit()
        session.close()

        print("User %s created successfully." % username)
    except Exception as e:
        print("Could not create user. %s" % e)


def create_dbnd_pool(pool_name):
    from airflow.utils.db import create_session
    from airflow.models import Pool

    print("Creating databand pool '%s'" % pool_name)
    with create_session() as session:
        dbnd_pool = Pool(pool=pool_name, slots=-1)
        session.merge(dbnd_pool)
