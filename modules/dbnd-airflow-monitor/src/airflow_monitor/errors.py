from dbnd._core.errors import DatabandError


class AirflowFetchingException(DatabandError):
    _default_show_exc_info = False


def failed_to_decode_data_From_airflow(url, nested_exception, data_sample=None):
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
        help_msg="Make sure that the server is up and running and was configured correctly.",
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
        help_msg="If the url is correct, this may be a problem in the network",
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
        help_msg="If the server url is correct, maybe the server was incorrectly configured with RBAC API mode.",
    )
