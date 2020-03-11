import getpass

from airflow import DAG, models, settings


def safe_get_context_manager_dag():
    """
    Try to find the CONTEXT_MANAGER_DAG object inside airflow.
    It was moved between versions, so we look for it in all the hiding places that we know of.
    """
    if hasattr(settings, "CONTEXT_MANAGER_DAG"):
        return settings.CONTEXT_MANAGER_DAG
    elif hasattr(DAG, "_CONTEXT_MANAGER_DAG"):
        return DAG._CONTEXT_MANAGER_DAG
    elif hasattr(models, "_CONTEXT_MANAGER_DAG"):
        return models._CONTEXT_MANAGER_DAG
    return None


def hostname_as_username():
    """
    This function is used to speed up airflow.
    When not redirected to this method, airflow retrieves the host name of the machine using the `socket` library.
    The calls to `socket` are extremely slow, so we redirect it to the native python implementation
    """
    return getpass.getuser()
