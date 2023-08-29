# © Copyright Databand.ai, an IBM Company 2022

from airflow.plugins_manager import AirflowPlugin

from dbnd_airflow.entrypoint import dbnd_airflow_tracking_entrypoint


# This code will run every time Airflow imports our plugin
dbnd_airflow_tracking_entrypoint()


class DbndAutoTracking(AirflowPlugin):
    name = "dbnd_airflow_auto_tracking"
