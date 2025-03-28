# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional
from unittest.mock import patch

from airflow_monitor.adapter.airflow_adapter import AirflowAdapter
from airflow_monitor.adapter.db_data_fetcher import DbFetcher
from airflow_monitor.config_data import AirflowIntegrationConfig
from dbnd_monitor.adapter import ThirdPartyInfo


class MockAirflowAdapter(AirflowAdapter):
    def __init__(self, config: AirflowIntegrationConfig, data_fetcher: DbFetcher):
        with patch(
            "airflow_monitor.adapter.airflow_adapter.DbFetcher",
            return_value=data_fetcher,
        ):
            super().__init__(config)

        self.metadata = None
        self.error_list = []

    def get_third_party_info(self) -> Optional[ThirdPartyInfo]:
        return ThirdPartyInfo(metadata=self.metadata, error_list=self.error_list)
