# © Copyright Databand.ai, an IBM Company 2022

import mock

from dbnd import dbnd_cmd


@mock.patch(
    "dbnd._core.tracking.backends.tracking_store_composite.CompositeTrackingStore.is_ready"
)
def test_tracker_wait(mock_composite_store):
    # works without global tracker, as all tests runs with [file,console] trackers only

    mock_composite_store.side_effect = [False, False, True, True]
    dbnd_cmd("tracker", ["wait"])
    assert mock_composite_store.call_count == 4
