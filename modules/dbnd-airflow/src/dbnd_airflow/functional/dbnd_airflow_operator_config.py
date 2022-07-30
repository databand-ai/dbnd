# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.task import Config


class DbndAirflowOperatorConfig(Config):
    _conf__task_family = "airflow_operator"
