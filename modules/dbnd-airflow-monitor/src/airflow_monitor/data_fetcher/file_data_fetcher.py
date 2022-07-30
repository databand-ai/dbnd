# © Copyright Databand.ai, an IBM Company 2022

import json
import logging
import os

from typing import List, Optional

from airflow_monitor.common.airflow_data import (
    AirflowDagRunsResponse,
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
)
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.data_fetcher.base_data_fetcher import AirflowDataFetcher


logger = logging.getLogger(__name__)


class FileFetcher(AirflowDataFetcher):
    def get_last_seen_values(self) -> LastSeenValues:
        data = self._get_data("get_last_seen_values")
        return LastSeenValues.from_dict(data)

    def get_airflow_dagruns_to_sync(
        self,
        last_seen_dag_run_id: Optional[int],
        last_seen_log_id: Optional[int],
        extra_dag_run_ids: Optional[List[int]],
        dag_ids: Optional[str],
    ) -> AirflowDagRunsResponse:
        data = self._get_data("get_airflow_dagruns_to_sync")
        return AirflowDagRunsResponse.from_dict(data)

    def get_full_dag_runs(
        self, dag_run_ids: List[int], include_sources: bool
    ) -> DagRunsFullData:
        data = self._get_data("get_full_dag_runs")
        return DagRunsFullData.from_dict(data)

    def get_dag_runs_state_data(self, dag_run_ids: List[int]) -> DagRunsStateData:
        data = self._get_data("get_dag_runs_state_data")
        return DagRunsStateData.from_dict(data)

    def is_alive(self):
        return True

    def __init__(self, config):
        # type: (AirflowServerConfig) -> FileFetcher
        super(FileFetcher, self).__init__(config)
        self.env = "JsonFile"
        self.json_file_path = config.json_file_path

    def _get_data(self, name):
        if not self.json_file_path:
            raise Exception(
                "'json_file_path' was not set in AirflowMonitor configuration."
            )

        try:
            with open(os.path.join(self.json_file_path, name + ".json")) as f:
                data = json.load(f)
                return data
        except Exception as e:
            logger.exception("Could not read json file {}".format(self.json_file_path))
            raise e

    def get_source(self):
        return self.json_file_path
