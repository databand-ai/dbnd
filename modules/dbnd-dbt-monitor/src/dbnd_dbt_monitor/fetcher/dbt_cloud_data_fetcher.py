import logging

from dbnd._core.tracking.dbt import get_run_data_from_dbt
from dbnd.utils.dbt_cloud_api_client import DbtCloudApiClient


logger = logging.getLogger(__name__)

MAX_BATCH_SIZE_FOR_IN_PROGRESS_RUNS = 30
DEFAULT_BATCH_SIZE = 30


class DbtCloudDataFetcher:
    def __init__(self, dbt_cloud_api_client, batch_size, job_id=None):
        self._dbt_cloud_client = dbt_cloud_api_client
        self._batch_size = batch_size
        self._job_id = job_id

    @classmethod
    def create_from_dbt_credentials(
        cls,
        dbt_cloud_api_token,
        dbt_cloud_account_id,
        batch_size=DEFAULT_BATCH_SIZE,
        job_id=None,
    ):
        dbt_cloud_api_client = DbtCloudApiClient(
            account_id=dbt_cloud_account_id, dbt_cloud_api_token=dbt_cloud_api_token
        )

        return cls(
            dbt_cloud_api_client=dbt_cloud_api_client,
            batch_size=batch_size,
            job_id=job_id,
        )

    def _get_last_dbt_run_ids(self, last_synced_run_id):
        offset = 0
        result_run_ids = []

        # Iterate over the runs going backwards, note that the ids are not sequential
        while True:
            last_runs = self._dbt_cloud_client.list_runs(
                self._batch_size, offset, "-created_at", self._job_id
            )
            if not last_runs:
                break

            new_run_ids = [run["id"] for run in last_runs]
            min_new_run_id = min(new_run_ids)

            if min_new_run_id < last_synced_run_id:
                result_run_ids.extend(
                    [run_id for run_id in new_run_ids if run_id >= last_synced_run_id]
                )
                break

            if min_new_run_id == last_synced_run_id:
                result_run_ids.extend(new_run_ids)
                break

            result_run_ids.extend(new_run_ids)
            offset += self._batch_size

        return result_run_ids

    def _get_run_ids_in_progress(self, limit):
        runs = self._dbt_cloud_client.list_runs(limit, 0, "status", self._job_id)
        return [run["id"] for run in runs if run["in_progress"]]

    def get_run_ids_to_sync_from_dbt(self, dbnd_last_run_id):
        all_run_ids = []
        new_run_ids = self._get_last_dbt_run_ids(dbnd_last_run_id)
        all_run_ids.extend(new_run_ids)

        return all_run_ids

    def get_last_seen_run_id(self):
        last_run = self._dbt_cloud_client.list_runs(1, 0, "-created_at", self._job_id)
        return last_run[0]["id"]

    def get_full_dbt_runs(self, dbt_run_ids):
        dbt_full_runs = []

        for run_id in dbt_run_ids:
            run_data = get_run_data_from_dbt(self._dbt_cloud_client, run_id)
            dbt_full_runs.append(run_data)

        return dbt_full_runs
