# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.common.base_component import BaseMonitorSyncer, start_syncer
from airflow_monitor.common.errors import capture_monitor_exception
from airflow_monitor.tracking_service.web_tracking_service import (
    AirflowDbndTrackingService,
)


logger = logging.getLogger(__name__)


class AirflowRuntimeConfigUpdater(BaseMonitorSyncer):
    SYNCER_TYPE = "runtime_config_updater"

    def __init__(self, *args, **kwargs):
        super(AirflowRuntimeConfigUpdater, self).__init__(*args, **kwargs)
        self.sync_last_heartbeat = True

    @property
    def sleep_interval(self):
        return self.config.config_updater_interval

    @capture_monitor_exception("sync_once")
    def _sync_once(self):
        try:
            from dbnd_airflow.export_plugin.api_functions import (
                check_syncer_config_and_set,
            )
        except ModuleNotFoundError:
            return

        self.tracking_service: AirflowDbndTrackingService
        dbnd_response = self.tracking_service.get_syncer_info()
        if dbnd_response:
            check_syncer_config_and_set(dbnd_response)


def start_runtime_config_updater(tracking_source_uid, run=True):
    return start_syncer(
        AirflowRuntimeConfigUpdater, tracking_source_uid=tracking_source_uid, run=run
    )
