# © Copyright Databand.ai, an IBM Company 2022
import logging

from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

from dbnd_datastage_monitor.datastage_client.datastage_api_client import (
    DataStageApiHttpClient,
    raise_if_not_found,
)
from dbnd_datastage_monitor.metrics.prometheus_metrics import report_error

from dbnd._core.log.external_exception_logging import log_exception_to_server


logger = logging.getLogger(__name__)


class DataStageAssetsClient:
    def __init__(self, client: DataStageApiHttpClient):
        self.client = client
        self.runs = []
        self.jobs = {}
        self.flows = {}
        self.failed_run_requests = []

    @property
    def project_id(self):
        return self.client.project_id

    def get_new_runs(
        self, start_time: str, end_time: str, next_page: Dict[str, any]
    ) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
        try:
            new_runs, next_page = self.client.get_runs_ids(
                start_time=start_time, end_time=end_time, next_page=next_page
            )
            return new_runs, next_page
        except Exception as e:
            logger.exception(
                "Error occurred during fetching new DataStage runs: %s", str(e)
            )
            if self.client.log_exception_to_webserver:
                log_exception_to_server(e, "datastage-monitor")
            report_error("get_new_runs", str(e))
            return None, None

    def clear_response_cache(self):
        # clears response cache to re-use getter
        self.runs = []
        self.flows = {}
        self.jobs = {}
        self.failed_run_requests = []

    def get_full_run(self, run_link: str):
        try:
            run_info = raise_if_not_found(
                value=self.client.get_run_info(run_link),
                error_message="unable to add run, run info attribute is not found",
            )

            ds_run = {"run_link": run_link, "cp4d_host": self.client.host_name}
            run_metadata = raise_if_not_found(
                value=run_info.get("metadata"),
                error_message="Unable to add run, no metadata attribute found",
            )

            run_id = run_metadata.get("asset_id")
            run_entity = raise_if_not_found(
                value=run_info.get("entity"),
                error_message="Unable to add run, no entity attribute found",
            )

            job_run = raise_if_not_found(
                value=run_entity.get("job_run"),
                error_message="Unable to add run, no job run attribute found",
            )

            job_id = job_run.get("job_ref")
            run_logs = self.client.get_run_logs(job_id=job_id, run_id=run_id)
            job_info = self.jobs.get(job_id)

            # request job if not exists in cache
            if not job_info:
                job_info = raise_if_not_found(
                    value=self.client.get_job(job_id),
                    error_message=f"Unable to get flow id for {job_id}, job_info was not found",
                )
                self.jobs[job_id] = job_info

            flow_id = job_info.get("entity").get("job").get("asset_ref")
            if not self.flows.get(flow_id):
                # request flow if not exists in cache
                flow = self.client.get_flow(flow_id)
                self.flows[flow_id] = flow

            ds_run["run_info"] = run_info
            ds_run["run_logs"] = run_logs
            self.runs.append(ds_run)
        except Exception as e:
            logger.exception(
                "Error occurred during fetching DataStage run: %s, Exception: %s",
                run_link,
                str(e),
            )
            self._submit_failed_runs(e, run_link)

    def _submit_failed_runs(self, e, run_link):
        # submit failed run to runs error handler
        logger.warning("append run link %s to failed run requests", run_link)
        self.failed_run_requests.append(run_link)
        if self.client.log_exception_to_webserver:
            log_exception_to_server(e, "datastage-monitor")
        report_error("get_full_run", str(e))

    def get_full_runs(self, runs_links: List[str]) -> Dict:
        for run in runs_links:
            self.get_full_run(run)
        return self.build_runs_response()

    def build_runs_response(self):
        response = {
            "runs": self.runs,
            "assets": {
                "jobs": self.jobs,
                "flows": self.flows,
                "connections": self.client.get_connections(),
            },
        }
        failed_run_requests = self.failed_run_requests
        self.clear_response_cache()
        return response, failed_run_requests


class ConcurrentRunsGetter(DataStageAssetsClient):
    def __init__(self, client, number_of_threads=10):
        super(ConcurrentRunsGetter, self).__init__(client=client)
        self._number_of_threads = number_of_threads

    def get_full_runs(self, runs_links: List[str]):
        with ThreadPoolExecutor(max_workers=self._number_of_threads) as executor:
            executor.map(self.get_full_run, runs_links)
        return self.build_runs_response()
