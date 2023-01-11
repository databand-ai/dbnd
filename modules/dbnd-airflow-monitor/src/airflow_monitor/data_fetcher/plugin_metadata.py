# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.common.airflow_data import PluginMetadata
from airflow_monitor.shared.base_monitor_config import NOTHING


def get_plugin_metadata() -> PluginMetadata:
    try:
        from airflow import version as airflow_version

        import dbnd_airflow

        from dbnd_airflow.export_plugin.compat import get_api_mode
        from dbnd_airflow.utils import get_airflow_instance_uid

        return PluginMetadata(
            airflow_version=airflow_version.version,
            plugin_version=dbnd_airflow.__version__,
            airflow_instance_uid=get_airflow_instance_uid(),
            api_mode=get_api_mode(),
        )
    except Exception:
        return PluginMetadata(
            airflow_version=NOTHING,
            plugin_version=NOTHING,
            airflow_instance_uid=NOTHING,
            api_mode=NOTHING,
        )
