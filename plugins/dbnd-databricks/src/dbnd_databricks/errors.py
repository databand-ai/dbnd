from dbnd._core.errors import DatabandRuntimeError


def failed_to_submit_databricks_job(nested_exception):
    return DatabandRuntimeError(
        "Databricks submit request failed with code %s." % nested_exception.status_code,
        show_exc_info=False,
        nested_exceptions=nested_exception,
        help_msg="Check your databricks connection id, cluster url and access token",
    )


def failed_to_run_databricks_job(status_code, error_message, log_url):
    return DatabandRuntimeError(
        "Databricks run failed with code %s." % status_code,
        show_exc_info=False,
        nested_exceptions=error_message,
        help_msg="Check cluster log for more info: %s." % log_url,
    )
