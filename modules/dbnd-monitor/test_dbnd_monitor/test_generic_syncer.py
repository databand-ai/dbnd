# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd_monitor.adapter.adapter import AssetState, AssetToState
from dbnd_monitor.generic_syncer import (
    GenericSyncer,
    assets_to_str,
    get_data_dimension_str,
)

from .conftest import MockTrackingService


class TestGenericSyncer:
    def test_sync_get_data_with_pagination(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (1, "active")
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (2, "active")
        assert mock_tracking_service.sent_data == [
            {"data": [0]},
            {"data": [1]},
            {"data": [2]},
        ]

    def test_sync_get_data_exception_on_save_data(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        # will cause exception in save_tracking_data
        # exception in save_tracking_data shouldn't prevent updating cursor
        mock_tracking_service.set_error(Exception("test"))
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        generic_runtime_syncer.sync_once()
        # last cursor is not updated after failure
        assert mock_tracking_service.get_last_cursor_and_state() == (1, "active")
        generic_runtime_syncer.sync_once()
        # call get data with same cursor before failure
        assert mock_tracking_service.get_last_cursor_and_state() == (2, "active")
        # every request appears twice because of retries
        assert mock_tracking_service.sent_data == [
            {"data": [0]},
            {"data": [0]},
            {"data": [1]},
            {"data": [1]},
            {"data": [2]},
            {"data": [2]},
        ]

    def test_sync_get_and_update_data_with_pagination(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        mock_tracking_service.set_active_runs(
            [
                {"asset_uri": 3, "state": "active"},
                {"asset_uri": 4, "state": "active"},
                {"asset_uri": 5, "state": "active"},
                {"asset_uri": 6, "state": "active"},
                {"asset_uri": 7, "state": "active"},
                {"asset_uri": 8, "state": "active"},
            ]
        )
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        assert mock_tracking_service.assets_state == [
            [
                AssetToState(asset_id=3, state=AssetState.FINISHED),
                AssetToState(asset_id=4, state=AssetState.FINISHED),
                AssetToState(asset_id=5, state=AssetState.FINISHED),
                AssetToState(asset_id=6, state=AssetState.FINISHED),
                AssetToState(asset_id=7, state=AssetState.FINISHED),
                AssetToState(asset_id=8, state=AssetState.FINISHED),
            ],
            [AssetToState(asset_id=0, state=AssetState.INIT)],
            [AssetToState(asset_id=0, state=AssetState.FINISHED)],
        ]
        assert mock_tracking_service.sent_data == [
            {"data": [3, 4, 5, 6, 7, 8]},
            {"data": [0]},
        ]

    def test_sync_get_and_update_data_with_pagination_and_unknown_state(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        mock_tracking_service.set_active_runs(
            [
                {"asset_uri": 3, "state": "wow", "data": {"retry_count": 2}},
                {"asset_uri": 4, "state": "active"},
                {"asset_uri": 5, "state": "active"},
                {"asset_uri": 6, "state": "active"},
                {"asset_uri": 7, "state": "active"},
                {"asset_uri": 8, "state": "active"},
            ]
        )
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        assert mock_tracking_service.assets_state == [
            [
                AssetToState(asset_id=4, state=AssetState.FINISHED),
                AssetToState(asset_id=5, state=AssetState.FINISHED),
                AssetToState(asset_id=6, state=AssetState.FINISHED),
                AssetToState(asset_id=7, state=AssetState.FINISHED),
                AssetToState(asset_id=8, state=AssetState.FINISHED),
            ],
            [AssetToState(asset_id=0, state=AssetState.INIT)],
            [AssetToState(asset_id=0, state=AssetState.FINISHED)],
        ]
        assert mock_tracking_service.sent_data == [
            {"data": [4, 5, 6, 7, 8]},
            {"data": [0]},
        ]

    def test_sync_get_and_update_data_with_pagination_and_failed_request(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        mock_tracking_service.set_active_runs(
            [
                {"asset_uri": 3, "state": "failed_request", "data": {"retry_count": 1}},
                {"asset_uri": 4, "state": "active"},
                {"asset_uri": 5, "state": "active"},
                {"asset_uri": 6, "state": "active"},
                {"asset_uri": 7, "state": "active"},
                {"asset_uri": 8, "state": "active"},
            ]
        )
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        assert mock_tracking_service.assets_state == [
            [
                AssetToState(asset_id=4, state=AssetState.FINISHED),
                AssetToState(asset_id=5, state=AssetState.FINISHED),
                AssetToState(asset_id=6, state=AssetState.FINISHED),
                AssetToState(asset_id=7, state=AssetState.FINISHED),
                AssetToState(asset_id=8, state=AssetState.FINISHED),
            ],
            [AssetToState(asset_id=3, state=AssetState.FAILED_REQUEST, retry_count=2)],
            [AssetToState(asset_id=0, state=AssetState.INIT)],
            [AssetToState(asset_id=0, state=AssetState.FINISHED)],
        ]


@pytest.mark.parametrize(
    "data, expected",
    [
        (None, "None"),
        ([], "[]"),
        (
            [AssetToState(1), AssetToState("asd"), AssetToState("/a/b/c")],
            "1,asd,/a/b/c",
        ),
    ],
)
def test_assets_to_str(data, expected):
    assert assets_to_str(data) == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        (None, "None"),
        ("hello", "5"),  # shouldn't happen but just in case
        (123, "1"),  # shouldn't happen but just in case
        ([], "[]"),
        ([1, 2, 5], "3"),
        ({"runs": [1, [], 3], "jobs": {"hello": 1232, "world": {}}}, "runs:3, jobs:2"),
    ],
)
def test_get_data_dimension_str(data, expected):
    assert get_data_dimension_str(data) == expected
