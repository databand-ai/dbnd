# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.shared.base_tracking_service import WebServersConfigurationService


class DataStageSyncersConfigurationService(WebServersConfigurationService):
    def _get_monitor_config_data(self):
        result = self._api_client.api_request(
            endpoint="datastage_syncers", method="GET", data=None
        )
        return result
