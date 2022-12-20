# © Copyright Databand.ai, an IBM Company 2022

import json
import logging

from typing import Dict, List

import pytest

from dbnd_datastage_monitor.datastage_client.datastage_api_client import (
    DataStageApiClient,
)
from dbnd_datastage_monitor.datastage_client.datastage_assets_client import (
    ConcurrentRunsGetter,
    DataStageAssetsClient,
)

from dbnd import relative_path
from dbnd._core.utils.date_utils import parse_datetime


logger = logging.getLogger(__name__)


class DataStageApiInMemoryClient(DataStageApiClient):
    def __init__(self, project_id):
        self.project_id = project_id
        self.flow_response_map = {}
        self.run_info_response_map = {}
        self.logs_response_map = {}
        self.connections_response_map = {}
        self.jobs_response_map = {}

    def add_flows_response(self, flows: List[Dict[str, any]]):
        for flow in flows:
            self.add_flow_response(flow)

    def add_flow_response(self, flow: Dict[str, any]):
        metadata = flow.get("metadata")
        if not metadata:
            logger.info("Unable to add flow, no metadata attribute found")
            return

        flow_id = metadata.get("asset_id")
        project_id = metadata.get("project_id")
        if not flow_id and project_id:
            logger.info(
                f"Missing identifiers flow_id: {flow_id} , project_id: {project_id}"
            )
            return

        self.flow_response_map[flow_id, project_id] = flow

    def get_connections(self):
        return self.connections_response_map

    def add_run_info_response(self, run_info: Dict[str, any]):
        metadata = run_info.get("metadata")
        if not metadata:
            logger.info("Unable to add run, no metadata attribute found")
            return
        project_id = metadata.get("project_id")
        project_runs = self.run_info_response_map.get(project_id)
        if project_runs is None:
            project_runs = []
            project_runs.append(run_info)
            self.run_info_response_map[project_id] = project_runs
        else:
            project_runs.append(run_info)

    def add_runs_info_response(self, runs_info: List[Dict[str, any]]):
        for run_info in runs_info:
            self.add_run_info_response(run_info)

    def add_job_response(self, job: Dict[str, any]):
        metadata = job.get("metadata")
        if not metadata:
            logger.info("Unable to add run, no metadata attribute found")
            return
        job_id = metadata.get("asset_id")
        project_id = metadata.get("project_id")
        self.jobs_response_map[project_id, job_id] = job

    def get_job(self, job_id):
        return self.jobs_response_map[self.project_id, job_id]

    def add_connections_response(self, connections: List[Dict[str, any]]):
        for connection in connections:
            self.add_connection_response(connection)

    def add_connection_response(self, connection: Dict[str, any]):
        metadata = connection.get("metadata")
        if not metadata:
            logger.info("Unable to add run, no metadata attribute found")
            return
        connection_id = metadata.get("asset_id")
        self.connections_response_map[connection_id] = connection

    def add_jobs_response(self, jobs: List[Dict[str, any]]):
        for job in jobs:
            self.add_job_response(job)

    def create_run_page(self, runs, page_size, next_page=0):
        runs_page = {}
        record_count = 0
        for k, v in runs.items():
            if next_page == 0 or page_size <= record_count <= next_page:
                runs_page[k] = v
            record_count += 1
            if record_count >= page_size + next_page:
                break
        return runs_page

    def get_runs_ids(
        self,
        start_time: str,
        end_time: str,
        next_page: Dict[str, any] = None,
        page_size=200,
    ):
        runs = {}
        min_start_time = parse_datetime(start_time)
        max_end_time = parse_datetime(end_time)
        project_runs = self.run_info_response_map.get(self.project_id)

        for run_info in project_runs:
            metadata = run_info.get("metadata")
            if not metadata:
                logger.info("Unable to add run, no metadata attribute found")
                return
            run_id = metadata.get("asset_id")
            run_start_time = parse_datetime(metadata.get("created_at"))
            run_end_time = parse_datetime(metadata.get("usage").get("last_updated_at"))

            if run_start_time >= min_start_time and run_end_time <= max_end_time:
                runs[run_id] = run_info.get("href")
        if not next_page:
            response = self.create_run_page(runs, page_size)
            return_next = page_size
        else:
            next_page = next_page + page_size
            response = self.create_run_page(runs, page_size, next_page)
            return_next = None
        return response, return_next

    def get_run_info(self, run: Dict[str, any]):
        return run

    def get_flow(self, flow_id):
        return self.flow_response_map.get((flow_id, self.project_id))

    def get_run_logs(self, job_id, run_id):
        return [{"log_1": "data"}, {"log_2": "data"}, {"log_3": "data"}]


class DataStageApiErrorClient(DataStageApiInMemoryClient):
    def __init__(self, project_id):
        super().__init__(project_id=project_id)

    def get_run_info(self, run: Dict[str, any]):
        raise Exception("test error")


def runs_getter():
    return DataStageAssetsClient(client=init_mock_client())


def error_runs_getter():
    return DataStageAssetsClient(client=init_error_client())


def concurent_runs_getter():
    return ConcurrentRunsGetter(client=init_mock_client())


def error_concurent_runs_getter():
    return ConcurrentRunsGetter(client=init_error_client())


def init_mock_client():
    mock_client = DataStageApiInMemoryClient(
        project_id="0ca4775d-860c-44f2-92ba-c7c8cfc0dd45"
    )
    mock_client.add_runs_info_response(
        json.load(open(relative_path(__file__, "mocks/runs_info.json")))
    )
    mock_client.add_flows_response(
        json.load(open(relative_path(__file__, "mocks/flows.json")))
    )
    mock_client.add_jobs_response(
        json.load(open(relative_path(__file__, "mocks/jobs.json")))
    )
    mock_client.add_connections_response(
        json.load(open(relative_path(__file__, "mocks/connections.json")))
    )
    return mock_client


def init_error_client():
    mock_error_client = DataStageApiErrorClient(
        project_id="0ca4775d-860c-44f2-92ba-c7c8cfc0dd45"
    )
    mock_error_client.add_runs_info_response(
        json.load(open(relative_path(__file__, "mocks/runs_info.json")))
    )
    return mock_error_client


class TestDataStageAssetsClient:
    @pytest.mark.parametrize(
        "datastage_runs_getter", [runs_getter(), concurent_runs_getter()]
    )
    def test_datastage_flow_fetcher_with_runs_in_date_range_with_pagination(
        self, datastage_runs_getter
    ):
        next_page = None
        all_runs = {}
        while True:
            runs, next_page = datastage_runs_getter.get_new_runs(
                start_time="2022-07-20T23:01:08Z",
                end_time="2022-09-30T23:02:50Z",
                next_page=next_page,
            )
            all_runs.update(runs)
            if not next_page:
                break

        response, failed_runs = datastage_runs_getter.get_full_runs(
            runs_links=list(all_runs.values())
        )
        expected_response = json.load(
            open(relative_path(__file__, "mocks/fetcher_response.json"))
        )

        assert set(expected_response) == set(response)

    @pytest.mark.parametrize(
        "datastage_runs_getter", [runs_getter(), concurent_runs_getter()]
    )
    def test_datastage_flow_fetcher_with_runs_not_in_date_range(
        self, datastage_runs_getter
    ):
        next_page = None
        all_runs = {}
        while True:
            runs, next_page = datastage_runs_getter.get_new_runs(
                start_time="2022-09-30T23:01:08Z",
                end_time="2022-09-30T23:02:50Z",
                next_page=next_page,
            )
            all_runs.update(runs)
            if not next_page:
                break

        response, failed_runs = datastage_runs_getter.get_full_runs(
            runs_links=list(all_runs.values())
        )
        assert not response["runs"]

    @pytest.mark.parametrize(
        "datastage_runs_getter", [error_runs_getter(), error_concurent_runs_getter()]
    )
    def test_failed_runs(self, datastage_runs_getter):
        next_page = None
        all_runs = {}
        while True:
            runs, next_page = datastage_runs_getter.get_new_runs(
                start_time="2022-07-20T23:01:08Z",
                end_time="2022-09-30T23:02:50Z",
                next_page=next_page,
            )
            all_runs.update(runs)
            if not next_page:
                break

        response, failed_runs = datastage_runs_getter.get_full_runs(
            runs_links=list(all_runs.values())
        )
        assert not response["runs"]
        failed_runs.sort()
        assert failed_runs == [
            "https://api.dataplatform.cloud.ibm.com/v2/assets/175a521f-6525-4d71-85e2-22544f8267a6?project_id=0ca4775d-860c-44f2-92ba-c7c8cfc0dd45",
            "https://api.dataplatform.cloud.ibm.com/v2/assets/8b62bbd1-e14f-4d44-9eb9-276098f762c0?project_id=0ca4775d-860c-44f2-92ba-c7c8cfc0dd45",
        ]
