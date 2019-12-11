from typing import List

from dbnd.errors import DatabandConfigError


def db_outdated():
    help_msg = "Please delete your current database and create new one by running `dbnd db init`"
    return DatabandConfigError(
        "Your database is outdated and could not be migrated with an up-to-date schema.",
        help_msg=help_msg,
        show_exc_info=False,
    )


def db_bad_alembic_revision(ex, current_version, supported_versions, local_db):
    # type: (object, str, List[str], bool) -> DatabandConfigError

    if local_db:
        action = "please upgrade your database"
    else:
        action = "please contact your admin to update databand db"

    help_msg = (
        "You are using out-dated version of DB, "
        "{action} to one of the following versions '{versions}' "
        "using `dbnd db init`".format(
            action=action, versions=", ".join(supported_versions)
        )
    )

    return DatabandConfigError(
        "Your database schema version %s is deprecated." % current_version,
        help_msg=help_msg,
        nested_exceptions=ex,
        show_exc_info=False,
    )


def db_connection_error(ex, connection_string):
    return DatabandConfigError(
        "Could not connect to database with the requested url: %s." % connection_string,
        help_msg="Please check your connection string and validate login parameters.",
        nested_exceptions=ex,
        show_exc_info=False,
    )


def db_no_dbnd_schema(ex, connection_string):
    return DatabandConfigError(
        "Databand schema does not exist within the requested database: %s."
        % connection_string,
        help_msg="Please initialize your database using `dbnd db init` and try again.",
        nested_exceptions=ex,
        show_exc_info=False,
    )


def db_multiple_alembic_head_revision(revisions):
    return DatabandConfigError(
        "You need to have only one HEAD alembic revision:  found revisions= %s."
        % revisions,
        help_msg="Please check your alembic migration folder.",
        show_exc_info=False,
    )


def db_cant_calculate_alembic_head_revisions(ex):
    return DatabandConfigError(
        "Can't calculate latest migration revision" % ex,
        help_msg="Please check your alembic migration folder .",
        nested_exceptions=ex,
        show_exc_info=False,
    )
