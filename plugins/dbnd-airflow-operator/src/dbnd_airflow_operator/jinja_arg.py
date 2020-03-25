from dbnd_airflow_operator.xcom_target import AirflowXComFileSystem
from targets.fs import register_file_system


class JinjaArg(str):
    def __new__(cls, value):
        obj = super(JinjaArg, cls).__new__(cls, value)
        return obj


register_file_system("jinja", AirflowXComFileSystem)
