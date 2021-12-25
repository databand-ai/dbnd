from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException


def is_rbac_enabled():
    try:
        return conf.get("webserver", "rbac").lower() == "true"
    except AirflowConfigException:
        return True


def base_log_folder():
    try:
        return conf.get("core", "base_log_folder")
    except AirflowConfigException:
        return conf.get("logging", "base_log_folder")


def get_task_log_reader():
    try:
        return conf.get("core", "task_log_reader")
    except AirflowConfigException:
        return conf.get("logging", "task_log_reader")


def get_api_mode():
    return "rbac" if is_rbac_enabled() else "flask-admin"
