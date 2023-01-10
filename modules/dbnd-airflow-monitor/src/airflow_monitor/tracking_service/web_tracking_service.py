# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import datetime, timedelta
from typing import List, Optional, Type

import airflow_monitor

from airflow_monitor.common.airflow_data import (
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
    MonitorState,
    PluginMetadata,
)
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.common.dbnd_data import DbndDagRunsResponse
from airflow_monitor.common.metric_reporter import generate_latest_metrics
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_tracking_service import (
    BaseDbndTrackingService,
    WebServersConfigurationService,
)
from dbnd._core.errors.base import DatabandConfigError
from dbnd._core.utils.timezone import utctoday


LONG_REQUEST_TIMEOUT = 300

logger = logging.getLogger(__name__)


def _min_start_time(start_time_window: int) -> Optional[datetime]:
    if not start_time_window:
        return None

    return utctoday() - timedelta(days=start_time_window)


class AirflowDbndTrackingService(BaseDbndTrackingService):
    def __init__(
        self,
        monitor_type: str,
        tracking_source_uid: str,
        server_monitor_config: Type[AirflowServerConfig],
        plugin_metadata: PluginMetadata,
    ):
        super(AirflowDbndTrackingService, self).__init__(
            monitor_type=monitor_type,
            tracking_source_uid=tracking_source_uid,
            server_monitor_config=server_monitor_config,
        )
        self.plugin_metadata = plugin_metadata
        self.airflow_monitor_version = airflow_monitor.__version__ + " v2"

    def update_last_seen_values(self, last_seen_values: LastSeenValues):
        self._make_request(
            "update_last_seen_values", method="POST", data=last_seen_values.as_dict()
        )

    def get_all_dag_runs(
        self, start_time_window: int, dag_ids: str
    ) -> DbndDagRunsResponse:
        params = {}
        start_time = _min_start_time(start_time_window)
        if start_time:
            params["min_start_time"] = start_time.isoformat()
        if dag_ids:
            params["dag_ids"] = dag_ids

        response = self._make_request(
            "get_all_dag_runs", method="GET", data=None, query=params
        )
        dags_to_sync = DbndDagRunsResponse.from_dict(response)

        return dags_to_sync

    def get_active_dag_runs(
        self, start_time_window: int, dag_ids: str
    ) -> DbndDagRunsResponse:
        params = {}
        start_time = _min_start_time(start_time_window)
        if start_time:
            params["min_start_time"] = start_time.isoformat()
        if dag_ids:
            params["dag_ids"] = dag_ids

        response = self._make_request(
            "get_running_dag_runs", method="GET", data=None, query=params
        )
        dags_to_sync = DbndDagRunsResponse.from_dict(response)

        return dags_to_sync

    def init_dagruns(
        self,
        dag_runs_full_data: DagRunsFullData,
        last_seen_dag_run_id: int,
        syncer_type: str,
        plugin_meta_data: PluginMetadata,
    ):
        data = dag_runs_full_data.as_dict()
        data["last_seen_dag_run_id"] = last_seen_dag_run_id
        data["syncer_type"] = syncer_type
        data["airflow_export_meta"] = plugin_meta_data.as_dict()
        response = self._make_request(
            "init_dagruns",
            method="POST",
            data=data,
            request_timeout=LONG_REQUEST_TIMEOUT,
        )
        return response

    def update_dagruns(
        self,
        dag_runs_state_data: DagRunsStateData,
        last_seen_log_id: int,
        syncer_type: str,
    ):
        data = dag_runs_state_data.as_dict()
        data["last_seen_log_id"] = last_seen_log_id
        data["syncer_type"] = syncer_type
        response = self._make_request(
            "update_dagruns",
            method="POST",
            data=data,
            request_timeout=LONG_REQUEST_TIMEOUT,
        )
        return response

    def get_syncer_info(self):
        return self._make_request("server_info", method="GET", data={})

    def set_starting_monitor_state(self):
        self.update_monitor_state(
            MonitorState(
                monitor_status="Scheduled",
                airflow_monitor_version=self.airflow_monitor_version,
                monitor_error_message=None,
            )
        )

    def _get_tracking_errors(self):
        from airflow_monitor.validations import (
            get_all_errors,
            get_tracking_validation_steps,
        )

        errors_list = get_all_errors(get_tracking_validation_steps())

        if len(errors_list) == 0:
            return None

        return ", ".join(errors_list)

    def set_running_monitor_state(self):
        error_message = self._get_tracking_errors()

        self.update_monitor_state(
            MonitorState(
                monitor_status="Running",
                airflow_monitor_version=self.airflow_monitor_version,
                airflow_version=self.plugin_metadata.airflow_version,
                airflow_export_version=self.plugin_metadata.plugin_version,
                airflow_instance_uid=self.plugin_metadata.airflow_instance_uid,
                monitor_error_message=error_message,
                api_mode=self.plugin_metadata.api_mode,
            )
        )


class AirflowSyncersConfigurationService(WebServersConfigurationService):
    def get_all_servers_configuration(
        self, monitor_config: Optional[AirflowMonitorConfig] = None
    ) -> List[BaseServerConfig]:
        servers = super(
            AirflowSyncersConfigurationService, self
        ).get_all_servers_configuration(monitor_config)

        if not monitor_config.syncer_name:
            return [s for s in servers if s.fetcher_type != "db"]
        else:
            filtered_servers = [
                s for s in servers if s.source_name == monitor_config.syncer_name
            ]
            if not filtered_servers:
                raise DatabandConfigError(
                    "No syncer configuration found matching name '%s'. Available syncers: %s"
                    % (
                        monitor_config.syncer_name,
                        ",".join([s.source_name for s in servers if s.source_name]),
                    ),
                    help_msg="Please provide correct syncer name (using --syncer-name parameter,"
                    " env variable DBND__AIRFLOW_MONITOR__SYNCER_NAME, or any other suitable way)",
                )
            return filtered_servers

    def send_metrics(self, monitor_config):
        metrics = generate_latest_metrics().decode("utf-8")
        self.send_prometheus_metrics(metrics, monitor_config.syncer_name)
