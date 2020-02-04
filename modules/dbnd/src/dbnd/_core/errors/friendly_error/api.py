from dbnd._core.errors.base import DatabandConnectionException


def api_connection_refused(connection_details, ex):
    return DatabandConnectionException(
        "Could not connect to databand api server on host: %s " % connection_details,
        show_exc_info=False,
        nested_exceptions=[ex],
        help_msg="1. Please check that databand server is up and accessible from client side.\n"
        "2. If you want to use databand in local mode with no server, "
        "please make sure 'dbnd_web' module is installed and use '--direct-db' flag "
        "or set core.tracker_api=db at configuration file for custom db path configuration.",
    )


def run_uid_not_found_in_db(run_uid):
    return DatabandConnectionException(
        "Could not find run uid '%s' in current database, are you using same DB for submission and execution?"
        % run_uid,
        show_exc_info=False,
        help_msg="Please check your configuration files, may be you are using local DB\n "
        "Validate that you don't have reference to previous run via --run-driver [UID] ",
    )
