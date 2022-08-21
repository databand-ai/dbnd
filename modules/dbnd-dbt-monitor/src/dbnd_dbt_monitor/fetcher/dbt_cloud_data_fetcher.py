# © Copyright Databand.ai, an IBM Company 2022

import logging

from functools import partial

from dbnd._core.tracking.dbt import get_run_data_from_dbt
from dbnd.utils.dbt_cloud_api_client import DbtCloudApiClient


logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 30


def get_last_run_ids(last_synced_run_id, batch_size, new_run_ids_getter):
    offset = 0
    result_run_ids = []

    # Iterate over the runs going backwards, note that the ids are not sequential
    while True:
        new_run_ids = new_run_ids_getter(offset)
        if not new_run_ids:
            break

        result_run_ids.extend(
            [run_id for run_id in new_run_ids if run_id > last_synced_run_id]
        )

        if min(new_run_ids) <= last_synced_run_id:
            break

        offset += batch_size

    return result_run_ids


class DbtCloudDataFetcher:
    def __init__(self, dbt_cloud_api_client, batch_size, job_id=None):
        self._dbt_cloud_client = dbt_cloud_api_client
        self._batch_size = batch_size
        self._job_id = job_id
        new_runs_getter = partial(
            self._dbt_cloud_client.list_runs,
            limit=self._batch_size,
            order_by="-created_at",
            job_id=self._job_id,
        )
        self._get_new_run_ids = lambda offset: [
            run["id"] for run in new_runs_getter(offset=offset)
        ]

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

    def get_run_ids_to_sync_from_dbt(self, dbnd_last_run_id):
        new_run_ids = get_last_run_ids(
            dbnd_last_run_id, self._batch_size, self._get_new_run_ids
        )

        return new_run_ids

    def get_last_seen_run_id(self):
        last_runs = self._dbt_cloud_client.list_runs(1, 0, "-created_at", self._job_id)
        if not last_runs:
            return None
        return last_runs[0]["id"]

    def get_full_dbt_runs(self, dbt_run_ids):
        dbt_full_runs = []

        for run_id in dbt_run_ids:
            run_data = get_run_data_from_dbt(self._dbt_cloud_client, run_id)
            dbt_full_runs.append(run_data)

        return dbt_full_runs
