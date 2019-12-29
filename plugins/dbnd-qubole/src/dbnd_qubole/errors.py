from dbnd._core.errors import DatabandRuntimeError


def failed_to_submit_qubole_job(nested_exception):
    return DatabandRuntimeError(
        "Qubole submit request failed with code %s." % nested_exception.status_code,
        show_exc_info=False,
        nested_exceptions=nested_exception,
        help_msg="Check your databricks connection id, cluster url and access token",
    )


def failed_to_run_qubole_job(status_code, log_url, spark_log):
    return DatabandRuntimeError(
        "Qubole run failed with code %s." % status_code,
        show_exc_info=False,
        nested_exceptions=spark_log,
        help_msg="Check spark log for more info: %s." % log_url,
    )
