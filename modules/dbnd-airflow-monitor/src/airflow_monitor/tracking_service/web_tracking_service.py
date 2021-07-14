import json
import logging
import typing

from datetime import datetime, timedelta
from typing import List

from airflow_monitor.common.airflow_data import (
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
    MonitorState,
)
from airflow_monitor.common.config_data import (
    AirflowServerConfig,
    TrackingServiceConfig,
)
from airflow_monitor.common.dbnd_data import DbndDagRunsResponse
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.tracking_service.base_tracking_service import (
    DbndAirflowTrackingService,
    ServersConfigurationService,
)
from dbnd._core.errors import DatabandConfigError
from dbnd._core.utils.timezone import utctoday


DEFAULT_REQUEST_TIMEOUT = 30
LONG_REQUEST_TIMEOUT = 300

logger = logging.getLogger(__name__)


if typing.TYPE_CHECKING:
    from dbnd.utils.api_client import ApiClient


def _get_api_client(tracking_service_config: TrackingServiceConfig) -> "ApiClient":
    from dbnd.utils.api_client import ApiClient

    if tracking_service_config.access_token:
        credentials = {"token": tracking_service_config.access_token}
    else:
        credentials = {
            "username": tracking_service_config.user,
            "password": tracking_service_config.password,
        }
    return ApiClient(
        tracking_service_config.url,
        credentials=credentials,
        default_request_timeout=DEFAULT_REQUEST_TIMEOUT,
    )


def _min_start_time(start_time_window: int) -> datetime:
    if not start_time_window:
        return None

    return utctoday() - timedelta(days=start_time_window)


class WebDbndAirflowTrackingService(DbndAirflowTrackingService):
    def __init__(
        self, tracking_source_uid, tracking_service_config: TrackingServiceConfig
    ):
        super().__init__(tracking_source_uid)
        self._api_client = _get_api_client(tracking_service_config)

    def _url_for(self, name):
        return "tracking/{}/{}".format(self.tracking_source_uid, name)

    def _make_request(self, name, method, data, query=None, request_timeout=None):
        return self._api_client.api_request(
            endpoint=self._url_for(name),
            method=method,
            data=data,
            query=query,
            request_timeout=request_timeout,
        )

    def update_last_seen_values(self, last_seen_values: LastSeenValues):
        self._make_request(
            "update_last_seen_values", method="POST", data=last_seen_values.as_dict(),
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
            "get_running_dag_runs", method="GET", data=None, query=params,
        )
        dags_to_sync = DbndDagRunsResponse.from_dict(response)

        return dags_to_sync

    def init_dagruns(
        self,
        dag_runs_full_data: DagRunsFullData,
        last_seen_dag_run_id: int,
        syncer_type: str,
    ):
        data = dag_runs_full_data.as_dict()
        data["last_seen_dag_run_id"] = last_seen_dag_run_id
        data["syncer_type"] = syncer_type
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

    def update_monitor_state(self, monitor_state: MonitorState):
        self._make_request(
            "update_monitor_state", method="POST", data=monitor_state.as_dict(),
        )

    def get_airflow_server_configuration(self) -> AirflowServerConfig:
        result_json = self._get_airflow_web_servers_data()
        if not result_json:
            raise DatabandConfigError(
                f"Not found configuration for syncer {self.tracking_source_uid}"
            )

        airflow_config = AirflowMonitorConfig()
        return AirflowServerConfig.create(airflow_config, result_json[0])

    def _get_airflow_web_servers_data(self):
        response = self._api_client.api_request(
            endpoint="airflow_web_servers",
            method="GET",
            data=None,
            query={
                "filter": json.dumps(
                    [
                        {
                            "name": "tracking_source_uid",
                            "op": "eq",
                            "val": str(self.tracking_source_uid),
                        }
                    ]
                )
            },
        )
        result_json = response["data"]
        return result_json


class WebServersConfigurationService(ServersConfigurationService):
    def __init__(self, tracking_service_config: TrackingServiceConfig):
        self._api_client = _get_api_client(tracking_service_config)

    def get_all_servers_configuration(
        self, airflow_config: AirflowMonitorConfig
    ) -> List[AirflowServerConfig]:
        result_json = self._get_airflow_web_servers_data()
        servers = [
            AirflowServerConfig.create(airflow_config, server) for server in result_json
        ]
        return servers

    def _get_airflow_web_servers_data(self):
        response = self._api_client.api_request(
            endpoint="airflow_web_servers", method="GET", data=None
        )
        result_json = response["data"]
        return result_json

    def send_prometheus_metrics(self, job_name: str, full_metrics: str):
        self._api_client.api_request(
            endpoint="tracking/save_monitor_metrics",
            method="POST",
            data={"job_name": job_name, "metrics": full_metrics},
        )
