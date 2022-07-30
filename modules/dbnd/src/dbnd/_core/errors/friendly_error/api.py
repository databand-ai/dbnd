# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.errors.base import DatabandConnectionException


def api_connection_refused(connection_details, ex):
    return DatabandConnectionException(
        "Connection Error: Could not connect to databand api server on host: %s "
        % connection_details,
        show_exc_info=False,
        nested_exceptions=[ex],
        help_msg="1. Please check that databand server is up and accessible from client side.\n"
        "2. You can temporary disable web tracking using --disable-web-tracker",
    )


def run_uid_not_found_in_db(run_uid):
    return DatabandConnectionException(
        "Could not find run uid '%s' in current database, are you using same DB for submission and execution?"
        % run_uid,
        show_exc_info=False,
        help_msg="Please check your configuration files, may be you are using local DB\n "
        "Validate that you don't have reference to previous run via --run-driver [UID] ",
    )


def couldnt_find_databand_run_in_db(name_or_uid, ex):
    return DatabandConnectionException(
        "Couldn't find the databand run '%s' in current database! Please validate that you are using the correct "
        "connection string" % name_or_uid,
        nested_exceptions=ex,
    )
