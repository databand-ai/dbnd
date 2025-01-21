# © Copyright Databand.ai, an IBM Company 2022

from collections import Counter
from typing import Dict, List

import prometheus_client as prom
import pytest

from dbnd_monitor.adapter import AssetState
from dbnd_monitor.generic_syncer import GenericSyncer
from dbnd_monitor.integration_metrics_reporter import (
    func_execution_time,
    integration_assets_in_state,
    integration_total_assets_size,
)

from .conftest import INTEGRATION_UID, MockAdapter, MockTrackingService


def get_metric_value(
    metrics: List[prom.Metric], sample_name: str, labels: Dict[str, str]
) -> float:
    for metric in metrics:
        for sample in metric.samples:
            if sample.name == sample_name and sample.labels == labels:
                return sample.value
    return 0


class TestGenericSyncerMetrics:
    INTEGRATION_LABELS = {
        "integration_id": str(INTEGRATION_UID),
        "tracking_source_uid": "12345",
        "syncer_type": "integration",
    }

    @pytest.fixture(autouse=True)
    def cleanup_metrics(self):
        integration_total_assets_size.clear()
        integration_assets_in_state.clear()
        func_execution_time.clear()

    @classmethod
    def get_new_assets_metric_value(cls) -> float:
        return get_metric_value(
            integration_total_assets_size.collect(),
            "dbnd_integration_assets_total_assets_size_total",
            {**cls.INTEGRATION_LABELS, "asset_source": "get_new_assets"},
        )

    @classmethod
    def get_active_assets_metric_value(cls) -> float:
        return get_metric_value(
            integration_total_assets_size.collect(),
            "dbnd_integration_assets_total_assets_size_total",
            {**cls.INTEGRATION_LABELS, "asset_source": "get_active_assets"},
        )

    @classmethod
    def get_asset_in_state_metrics(cls) -> Counter:
        metrics_by_state = Counter()
        for state in AssetState:
            metrics_by_state[state] = int(
                get_metric_value(
                    integration_assets_in_state.collect(),
                    "dbnd_integration_assets_in_state_total",
                    {**cls.INTEGRATION_LABELS, "asset_state": state.value},
                )
            )
        return metrics_by_state

    @classmethod
    def get_func_execution_counts(cls):
        counts_by_func = {}
        for func_name in ("save_tracking_data", "get_assets_data"):
            counts_by_func[func_name] = get_metric_value(
                func_execution_time.collect(),
                "dbnd_integration_func_execution_time_count",
                {**cls.INTEGRATION_LABELS, "func_name": func_name},
            )
        return counts_by_func

    def test_metrics(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
        mock_adapter: MockAdapter,
    ):
        # all states will be 0
        expected_states_metric: Counter = self.get_asset_in_state_metrics()

        ### ITERATION 1
        generic_runtime_syncer.sync_once()
        # each iteration returns one new asset, and it will finish
        assert self.get_new_assets_metric_value() == 1

        expected_states_metric.update({AssetState.FINISHED: 1})
        assert self.get_asset_in_state_metrics() == expected_states_metric

        ### ITERATION 2
        generic_runtime_syncer.sync_once()
        # each iteration returns one new asset
        assert self.get_new_assets_metric_value() == 2

        expected_states_metric.update({AssetState.FINISHED: 1})
        assert self.get_asset_in_state_metrics() == expected_states_metric

        # no active assets so far
        assert self.get_active_assets_metric_value() == 0

        ### ITERATION 3
        # now let's return some active assets
        mock_tracking_service.set_active_runs(
            [
                {"asset_uri": 3, "state": "failed_request", "data": {"retry_count": 1}},
                {"asset_uri": 4, "state": "active"},
                {"asset_uri": 5, "state": "active"},
                {"asset_uri": 6, "state": "active"},
                {"asset_uri": 7, "state": "init"},
                {
                    "asset_uri": 3,
                    "state": "failed_request",
                    "data": {"retry_count": 10},
                },
            ]
        )
        generic_runtime_syncer.sync_once()
        assert self.get_new_assets_metric_value() == 3  # one more
        assert self.get_active_assets_metric_value() == 6

        # all non-failed will finish (mock adapter's logic)
        expected_states_metric.update(
            {
                AssetState.FINISHED: 1 + 4,  # 1 from "new assets" + 4 from "get active"
                AssetState.FAILED_REQUEST: 1,  # one regular failed above should stay failed
                AssetState.MAX_RETRY: 1,  # one failed above with retry=10 goes to max_retry
            }
        )
        assert self.get_asset_in_state_metrics() == expected_states_metric

        ### ITERATION 4
        mock_adapter.set_error(Exception("random"))
        # now all should fail
        generic_runtime_syncer.sync_once()
        assert self.get_new_assets_metric_value() == 4  # one more
        assert self.get_active_assets_metric_value() == 12  # 6 prev + 6 from this iter
        expected_states_metric.update(
            {
                AssetState.FAILED_REQUEST: 6,  # 1 from "new assets" + 5 from "get active"
                AssetState.MAX_RETRY: 1,  # one failed above with retry=10 goes to max_retry
            }
        )
        assert self.get_asset_in_state_metrics() == expected_states_metric

        ### ITERATION 5
        mock_adapter.set_error(None)
        # now let's tell adapter to return assets as is
        mock_adapter.get_assets_data = lambda assets: assets
        generic_runtime_syncer.sync_once()
        assert self.get_new_assets_metric_value() == 5  # one more
        assert self.get_active_assets_metric_value() == 18  # 6 prev + 6 from this iter

        expected_states_metric.update(
            {
                AssetState.INIT: 2,  # 1 from "new assets" + 1 from "get active"
                AssetState.ACTIVE: 3,
                AssetState.FAILED_REQUEST: 1,  # one regular failed above should stay failed
                AssetState.MAX_RETRY: 1,  # one failed above with retry=10 goes to max_retry
            }
        )
        assert self.get_asset_in_state_metrics() == expected_states_metric

        assert self.get_func_execution_counts() == {
            "get_assets_data": 14,
            "save_tracking_data": 6,
        }
