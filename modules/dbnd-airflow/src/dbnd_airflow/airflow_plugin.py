import logging

from airflow.plugins_manager import AirflowPlugin

from dbnd_airflow.web.airflow_app import use_databand_airflow_dagbag


logger = logging.getLogger()


class DatabandAirflowWebserverPlugin(AirflowPlugin):
    name = "databand_airflow_plugin"

    @classmethod
    def on_load(cls, *args, **kwargs):
        cls.patch_airflow_create_app()

    @staticmethod
    def patch_airflow_create_app():
        logger.info("Monkey patching Airflow to use DBND DagBag")
        from airflow.www_rbac import app as airflow_app
        from dbnd_airflow._plugin import configure_sql_alchemy_conn

        def patch_create_app(create_app_func):
            def patched_create_app(*args, **kwargs):
                from dbnd._core.configuration.dbnd_config import config

                logger.info("Setting SQL connection")
                config.load_system_configs()
                configure_sql_alchemy_conn()

                res = create_app_func(*args, **kwargs)
                use_databand_airflow_dagbag()
                return res

            return patched_create_app

        airflow_app.create_app = patch_create_app(airflow_app.create_app)
