from dbnd._core.errors import DatabandError


class AirflowFetchingException(DatabandError):
    _default_show_exc_info = False


def failed_to_decode_data_from_airflow(url, nested_exception, data_sample=None):
    message = "Could not decode the received data from %s, error in json format." % (
        url,
    )
    if data_sample:
        message += " Data received: %s" % (data_sample,)
    return AirflowFetchingException(
        message,
        show_exc_info=False,
        nested_exceptions=nested_exception,
        help_msg="Make sure that you configured the server correctly.",
    )


def failed_to_connect_to_airflow_server(url, nested_exception):
    return AirflowFetchingException(
        "Could not connect to Airflow web server url %s" % (url,),
        show_exc_info=False,
        nested_exceptions=nested_exception,
        help_msg="1. Make sure that Airflow server is up and running.\n"
        "2. Make sure that airflow server was configured correctly in databand webserver.\n"
        "3. In case you use docker, make sure dbnd-airflow-monitor is in the same network as Airflow server.",
    )


def failed_to_connect_to_server_port(url, nested_exception):
    return AirflowFetchingException(
        "Could not connect to Airflow web server url %s" % (url,),
        show_exc_info=False,
        nested_exceptions=nested_exception,
        help_msg="Make sure that the port you typed is in the correct port range.",
    )


def failed_to_fetch_from_airflow(url, nested_exception, error_code=None):
    message = "An error occurred while trying to fetch data from airflow server %s" % (
        url,
    )
    if error_code:
        message += " Error code: %s" % (error_code,)
    return AirflowFetchingException(
        message,
        show_exc_info=False,
        nested_exceptions=nested_exception,
        help_msg="1. Check that dbnd-airflow-export is installed on airflow webserver.\n"
        "2. Check that airflow server is configured with the right API mode in databand webserver.",
    )


def failed_to_login_to_airflow(url):
    message = "Could not login to Airflow server %s" % (url,)
    return AirflowFetchingException(
        message,
        show_exc_info=False,
        help_msg="Check that your Airflow server credentials are correct.",
    )


def failed_to_get_csrf_token(url):
    message = "Could not get required csrf token for logging to Airflow server %s" % (
        url,
    )
    return AirflowFetchingException(
        message,
        show_exc_info=False,
        help_msg="If the Airflow server url is correct, maybe the Airflow server was incorrectly configured with RBAC API mode.",
    )
