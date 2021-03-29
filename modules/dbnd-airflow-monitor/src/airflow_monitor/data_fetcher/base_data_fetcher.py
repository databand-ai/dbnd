from typing import List, Optional

from airflow_monitor.common import (
    AirflowDagRun,
    AirflowDagRunsResponse,
    AirflowServerConfig,
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
)


class AirflowDataFetcher(object):
    def __init__(self, config):
        # type: (AirflowServerConfig) -> None
        pass

    def get_last_seen_values(self) -> LastSeenValues:
        raise NotImplementedError()

    def get_airflow_dagruns_to_sync(
        self,
        last_seen_dag_run_id: Optional[int],
        last_seen_log_id: Optional[int],
        extra_dag_run_ids: Optional[List[int]],
    ) -> AirflowDagRunsResponse:
        raise NotImplementedError()

    def get_full_dag_runs(self, dagruns: List[AirflowDagRun]) -> DagRunsFullData:
        raise NotImplementedError()

    def get_dag_runs_state_data(self, dagruns: List[AirflowDagRun]) -> DagRunsStateData:
        raise NotImplementedError()

    def is_alive(self):
        raise NotImplementedError()


#
#
# import logging
#
# from urllib.parse import urlparse
#
# import prometheus_client
# import requests
# import six
#
# from airflow_monitor.airflow_servers_fetching import AirflowFetchingConfiguration
# from airflow_monitor.errors import (
#     AirflowFetchingException,
#     failed_to_connect_to_airflow_server,
#     failed_to_connect_to_server_port,
#     failed_to_decode_data_From_airflow,
#     failed_to_fetch_from_airflow,
#     failed_to_get_csrf_token,
#     failed_to_login_to_airflow,
# )
#
# import simplejson
#
# from bs4 import BeautifulSoup as bs
#
#
# if six.PY3:
#     from json import JSONDecodeError
# else:
#     from simplejson import JSONDecodeError
#
# logger = logging.getLogger(__name__)
