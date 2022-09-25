# © Copyright Databand.ai, an IBM Company 2022
import logging

from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple

from dbnd._core.log.external_exception_logging import log_exception_to_server


logger = logging.getLogger(__name__)


class DataStageAssetsClient:
    def __init__(self, client):
        self.client = client
        self.runs = []
        self.jobs = {}
        self.flows = {}

    def get_new_runs(
        self, start_time: str, end_time: str, next_page: Dict[str, any]
    ) -> Tuple[Dict[str, str], str]:
        return self.client.get_runs_ids(
            start_time=start_time, end_time=end_time, next_page=next_page
        )

    def clear_response_cache(self):
        # clears response cache to re-use getter
        self.runs = []
        self.flows = {}
        self.jobs = {}

    def get_full_run(self, run_link: str):
        try:
            # TODO: pagination on runs api when run_id is not specified
            run_info = self.client.get_run_info(run_link)
            if not run_info:
                return
            ds_run = {"run_link": run_link}
            run_metadata = run_info.get("metadata")
            if not run_metadata:
                logger.error("Unable to add run, no metadata attribute found")
                return
            run_id = run_metadata.get("asset_id")
            run_entity = run_info.get("entity")
            if not run_entity:
                logger.error("Unable to add run, no entity attribute found")
                return
            job_run = run_entity.get("job_run")
            if not job_run:
                logger.error("Unable to add run, no job run attribute found")
                return
            job_id = job_run.get("job_ref")
            # TODO: pagination on logs
            run_logs = self.client.get_run_logs(job_id=job_id, run_id=run_id)
            job_info = self.jobs.get(job_id)
            # request job if not exists in cache
            if not job_info:
                job_info = self.client.get_job(job_id)
                if not job_info:
                    logger.error(
                        f"Unable to get flow id for {job_id}, job_info was not found"
                    )
                    return
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
                "Error occurred during fetching DataStage run: %s", run_link
            )
            log_exception_to_server(e)

    def get_full_runs(self, runs_links: List[str]) -> Dict:
        for run in runs_links:
            self.get_full_run(run)
        response = {
            "runs": self.runs.copy(),
            "assets": {
                "jobs": self.jobs.copy(),
                "flows": self.flows.copy(),
                "connections": self.client.get_connections(),
            },
        }
        self.clear_response_cache()
        return response


class ConcurrentRunsGetter(DataStageAssetsClient):
    def __init__(self, client, number_of_threads=10):
        super(ConcurrentRunsGetter, self).__init__(client)
        self._number_of_threads = number_of_threads
        self.runs = []
        self.jobs = {}
        self.flows = {}

    def get_full_runs(self, runs_links: List[str]):
        with ThreadPoolExecutor(max_workers=self._number_of_threads) as executor:
            executor.map(self.get_full_run, runs_links)
        response = {
            "runs": self.runs.copy(),
            "assets": {
                "jobs": self.jobs.copy(),
                "flows": self.flows.copy(),
                "connections": self.client.get_connections(),
            },
        }
        self.clear_response_cache()
        return response
