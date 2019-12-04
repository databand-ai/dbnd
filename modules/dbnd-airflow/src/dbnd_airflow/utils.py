from dbnd._core.utils.project.project_fs import abs_join, relative_path


_airflow_lib_home_default = relative_path(__file__)


def dbnd_airflow_path(*path):
    return abs_join(_airflow_lib_home_default, *path)
