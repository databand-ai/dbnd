def get_windows_compatible_patches():
    import getpass
    import sys

    from airflow.utils import configuration as airflow_configuration, timeout

    from dbnd_airflow.airflow_override.dbnd_airflow_windows import timeout as timeout_patch
    from dbnd_airflow.airflow_override.dbnd_airflow_windows.configuration import tmp_configuration_copy
    from dbnd_airflow.airflow_override.dbnd_airflow_windows.getuser import find_runnable_getuser_function

    return (
        (timeout, "timeout", timeout_patch.timeout),
        (airflow_configuration, "tmp_configuration_copy", tmp_configuration_copy),
        (getpass, "getuser", find_runnable_getuser_function()),
        (sys, "maxint", sys.maxsize),
    )


def patch_airflow_windows_support():
    from dbnd._core.utils.object_utils import patch_models
    patch_models(get_windows_compatible_patches())
