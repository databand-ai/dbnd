# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional
from unittest.mock import patch
from uuid import uuid4

from airflow_monitor.adapter.airflow_adapter import AirflowAdapter
from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from dbnd_monitor.adapter.adapter import ThirdPartyInfo


class MockAirflowAdapter(AirflowAdapter):
    def __init__(self, config: AirflowIntegrationConfig, data_fetcher: DbFetcher):
        with patch(
            "airflow_monitor.adapter.airflow_adapter.DbFetcher",
            return_value=data_fetcher,
        ), patch(
            "airflow_monitor.adapter.airflow_adapter.get_or_create_airflow_instance_uid",
            return_value=uuid4(),
        ):
            super().__init__(config)

        self.metadata = None
        self.error_list = []

    def get_third_party_info(self) -> Optional[ThirdPartyInfo]:
        return ThirdPartyInfo(metadata=self.metadata, error_list=self.error_list)
