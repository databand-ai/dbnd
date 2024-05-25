# © Copyright Databand.ai, an IBM Company 2022
import logging

from typing import Optional

from airflow_monitor.adapter.airflow_adapter import AirflowAdapter
from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.config_updater.runtime_config_updater import (
    AirflowRuntimeConfigUpdater,
)
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from airflow_monitor.shared.adapter.adapter import ThirdPartyInfo
from airflow_monitor.shared.base_integration import BaseIntegration
from airflow_monitor.syncer.runtime_syncer import AirflowRuntimeSyncer
from airflow_monitor.tracking_service.airflow_tracking_service import (
    AirflowTrackingService,
)


logger = logging.getLogger(__name__)


class AirflowIntegration(BaseIntegration):
    MONITOR_TYPE = "airflow"
    CONFIG_CLASS = AirflowIntegrationConfig
    config: AirflowIntegrationConfig

    def get_components_dict(self):
        return {
            "state_sync": AirflowRuntimeSyncer,
            "config_updater": AirflowRuntimeConfigUpdater,
        }

    def get_components(self):
        tracking_service = self.get_tracking_service()
        data_fetcher = self.get_data_fetcher()
        components_dict = self.get_components_dict()
        all_components = []
        for _, syncer_class in components_dict.items():
            syncer_instance = syncer_class(
                config=self.config,
                tracking_service=tracking_service,
                reporting_service=self.reporting_service,
                data_fetcher=data_fetcher,
            )
            all_components.append(syncer_instance)

        return all_components

    def get_data_fetcher(self):
        return DbFetcher(self.config)

    def get_tracking_service(self) -> AirflowTrackingService:
        return AirflowTrackingService(
            monitor_type=self.MONITOR_TYPE,
            tracking_source_uid=str(self.config.tracking_source_uid),
        )

    def get_third_party_info(self) -> Optional[ThirdPartyInfo]:
        return AirflowAdapter().get_third_party_info()

    def on_integration_disabled(self):
        tracking_service = self.get_tracking_service()

        logger.info("Running runtime_config_updater last time before stopping")
        updater = AirflowRuntimeConfigUpdater(
            self.config, tracking_service, self.reporting_service
        )
        updater.sync_once()
