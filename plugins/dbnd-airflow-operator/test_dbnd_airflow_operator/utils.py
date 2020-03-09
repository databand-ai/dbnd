from dbnd import relative_path
from dbnd._core.utils.project.project_fs import abs_join


_lib_dbnd_airflow_operator_test = relative_path(__file__, "..", "..", "..")


def dbnd_airflow_operator_test_path(*path):
    return abs_join(_lib_dbnd_airflow_operator_test, *path)


def dbnd_airflow_operator_home_path(*path):
    return dbnd_airflow_operator_test_path("home", *path)
