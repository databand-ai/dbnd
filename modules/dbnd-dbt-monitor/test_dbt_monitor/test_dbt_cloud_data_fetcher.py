# © Copyright Databand.ai, an IBM Company 2022

import random

from dbnd_dbt_monitor.fetcher.dbt_cloud_data_fetcher import get_last_run_ids


# Reverse order to simulate dbt run ids starting from the biggest and going backwards
RUN_IDS = sorted(random.sample(range(10, 800), 50), reverse=True)
BATCH_SIZE = 5


def mock_new_run_ids_getter(offset):
    return RUN_IDS[offset : offset + BATCH_SIZE]


class TestDbtCloudDataFetcher:
    def test_get_last_run_ids(self):
        # Simulate an example where we fetch from a list and verify that we get exactly the slice
        # of the array that we expect (starting from index 0 and going until we reach last_synced_run_id)
        for index, last_synced_run_id in enumerate(RUN_IDS):
            result = get_last_run_ids(
                last_synced_run_id, BATCH_SIZE, mock_new_run_ids_getter
            )
            assert result == RUN_IDS[0:index]
