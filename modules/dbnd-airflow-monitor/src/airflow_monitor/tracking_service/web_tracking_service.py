import json
import logging
import typing

from typing import List

from airflow_monitor.common import (
    AirflowServerConfig,
    DagRunsFullData,
    DagRunsStateData,
    DbndDagRunsResponse,
    LastSeenValues,
    MonitorConfig,
    TrackingServiceConfig,
)
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.tracking_service.af_tracking_service import (
    DbndAirflowTrackingService,
    ServersConfigurationService,
)


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
    return ApiClient(tracking_service_config.url, credentials=credentials)


class WebDbndAirflowTrackingService(DbndAirflowTrackingService):
    def __init__(
        self, tracking_source_uid, tracking_service_config: TrackingServiceConfig
    ):
        super().__init__(tracking_source_uid)
        self._api_client = _get_api_client(tracking_service_config)

    def _url_for(self, name):
        return "tracking/{}/{}".format(self.tracking_source_uid, name)

    def update_last_seen_values(self, last_seen_values: LastSeenValues):
        response = self._api_client.api_request(
            endpoint=self._url_for("update_last_seen_values"),
            method="POST",
            data=last_seen_values.as_dict(),
        )

    def get_dbnd_dags_to_sync(
        self, max_execution_date_window: int
    ) -> DbndDagRunsResponse:
        response = self._api_client.api_request(
            endpoint=self._url_for("get_running_dag_runs"), method="GET", data=None,
        )
        dags_to_sync = DbndDagRunsResponse.from_dict(response)

        return dags_to_sync

    def init_dagruns(
        self, dag_runs_full_data: DagRunsFullData, last_seen_dag_run_id: int
    ):
        data = dag_runs_full_data.as_dict()
        data["last_seen_dag_run_id"] = last_seen_dag_run_id
        response = self._api_client.api_request(
            endpoint=self._url_for("init_dagruns"), method="POST", data=data
        )
        return response

    def update_dagruns(
        self, dag_runs_state_data: DagRunsStateData, last_seen_log_id: int
    ):
        data = dag_runs_state_data.as_dict()
        data["last_seen_log_id"] = last_seen_log_id
        response = self._api_client.api_request(
            endpoint=self._url_for("update_dagruns"), method="POST", data=data
        )
        return response

    def get_monitor_configuration(self) -> MonitorConfig:
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
        if not result_json:
            raise Exception()  # TODO
        if len(result_json) > 1:
            raise Exception()  # TODO

        airflow_config = AirflowMonitorConfig()
        return MonitorConfig.create(airflow_config, result_json[0])


class WebServersConfigurationService(ServersConfigurationService):
    def __init__(self, tracking_service_config: TrackingServiceConfig):
        self._api_client = _get_api_client(tracking_service_config)

    def get_all_servers_configuration(self) -> List[AirflowServerConfig]:
        airflow_config = AirflowMonitorConfig()
        response = self._api_client.api_request(
            endpoint="airflow_web_servers", method="GET", data=None
        )
        result_json = response["data"]
        servers = [
            AirflowServerConfig.create(airflow_config, server) for server in result_json
        ]
        return servers
        # except Exception as e:
        #     logger.error(
        #         "An error occurred while connecting to server: {}. Error: {}".format(
        #             self._api_client, e
        #         )
        #     )
        #     return []
