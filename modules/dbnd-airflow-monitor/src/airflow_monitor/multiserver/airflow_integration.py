# © Copyright Databand.ai, an IBM Company 2022
import logging

from typing import Optional

from airflow_monitor.adapter.airflow_adapter import AirflowAdapter
from airflow_monitor.adapter.db_data_fetcher import DbFetcher
from airflow_monitor.config_data import AirflowIntegrationConfig
from airflow_monitor.multiserver.airflow_tracking_service import AirflowTrackingService
from airflow_monitor.multiserver.runtime_config_updater import (
    AirflowRuntimeConfigUpdater,
)
from dbnd_monitor.adapter import ThirdPartyInfo
from dbnd_monitor.base_integration import BaseIntegration
from dbnd_monitor.base_integration_config import BaseIntegrationConfig
from dbnd_monitor.generic_syncer import GenericSyncer
from dbnd_monitor.reporting_service import ReportingService


logger = logging.getLogger(__name__)


class AirflowIntegration(BaseIntegration):
    MONITOR_TYPE = "airflow"
    CONFIG_CLASS = AirflowIntegrationConfig
    config: AirflowIntegrationConfig

    def __init__(
        self,
        integration_config: BaseIntegrationConfig,
        reporting_service: Optional[ReportingService] = None,
    ):
        super().__init__(integration_config, reporting_service)
        self.tracking_service = AirflowTrackingService(
            monitor_type=self.MONITOR_TYPE,
            tracking_source_uid=str(self.config.tracking_source_uid),
        )
        self.adapter = AirflowAdapter(self.config)

    def get_components(self):
        state_syncer = GenericSyncer(
            self.config,
            self.tracking_service,
            self.reporting_service,
            self.adapter,
            str(self.config.tracking_source_uid),
            self.config.source_name,
        )

        config_updater = AirflowRuntimeConfigUpdater(
            self.config,
            self.tracking_service,
            self.reporting_service,
            external_id=self.config.source_name,
        )

        return [state_syncer, config_updater]

    def get_third_party_info(self) -> Optional[ThirdPartyInfo]:
        return self.adapter.get_third_party_info()

    def on_integration_disabled(self):
        logger.info("Running runtime_config_updater last time before stopping")
        updater = AirflowRuntimeConfigUpdater(
            self.config, self.tracking_service, self.reporting_service
        )
        updater.sync_once()

    @staticmethod
    def get_source_instance_uid_or_none() -> Optional[str]:
        return DbFetcher.get_airflow_instance_uid()
