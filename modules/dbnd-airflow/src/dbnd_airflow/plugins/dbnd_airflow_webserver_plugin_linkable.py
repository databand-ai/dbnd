import logging

from airflow.plugins_manager import AirflowPlugin

from dbnd_airflow.airflow_override.dbnd_aiflow_webserver import patch_airflow_create_app


# WE SHOULD NOT HAVE ANY dbnd imports here -- circle import:  dbnd_airflow -> airflow -> load .. -> import dbnd_airflow
logger = logging.getLogger()


class DatabandAirflowWebserverPlugin(AirflowPlugin):
    name = "databand_airflow_plugin"

    # airflow doesn't call on_load for plugins loaded from disk, we call patch explicitly
    # @classmethod
    # def on_load(cls, *args, **kwargs):
    #     patch_airflow_create_app()


patch_airflow_create_app()
