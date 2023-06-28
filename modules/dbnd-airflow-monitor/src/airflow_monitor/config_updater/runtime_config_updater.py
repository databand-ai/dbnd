# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.tracking_service.airflow_tracking_service import (
    AirflowTrackingService,
)


logger = logging.getLogger(__name__)


class AirflowRuntimeConfigUpdater(BaseComponent):
    SYNCER_TYPE = "runtime_config_updater"

    @property
    def sleep_interval(self):
        return self.config.config_updater_interval

    def _sync_once(self):
        try:
            from dbnd_airflow.export_plugin.api_functions import (
                check_syncer_config_and_set,
            )
        except ModuleNotFoundError:
            return

        self.tracking_service: AirflowTrackingService
        dbnd_response = self.tracking_service.get_syncer_info()
        if dbnd_response:
            check_syncer_config_and_set(dbnd_response)
