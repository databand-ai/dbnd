# © Copyright Databand.ai, an IBM Company 2022

from mock import patch

from dbnd import get_databand_context, new_dbnd_context
from dbnd._core.errors.base import (
    DatabandSystemError,
    DatabandWebserverNotReachableError,
)
from dbnd._core.tracking.backends.channels.tracking_async_web_channel import (
    TrackingAsyncWebChannel,
)
from dbnd._core.tracking.backends.tracking_store_channels import (
    TrackingStoreThroughChannel,
)
from dbnd._core.utils.uid_utils import get_uuid
from dbnd._vendor import tenacity


class TestAsyncTracking:
    @patch("dbnd.utils.api_client.ApiClient.api_request")
    def test_thread_not_started_immideately(self, fake_api_request):
        ctx = get_databand_context()
        async_store = TrackingStoreThroughChannel.build_with_async_web_channel(ctx)
        assert async_store.is_ready()
        assert not async_store.channel._background_worker.is_alive

        async_store.heartbeat(get_uuid())
        assert async_store.channel._background_worker.is_alive

    @patch("dbnd.utils.api_client.ApiClient.api_request")
    def test_tracking_after_flush(self, fake_api_request):
        ctx = get_databand_context()
        async_store = TrackingStoreThroughChannel.build_with_async_web_channel(ctx)
        async_store.heartbeat(get_uuid())
        async_store.flush()
        async_store.heartbeat(get_uuid())
        async_store.flush()

    # this test is sometimes flacky with this exception:
    # dbnd._core.errors.base.DatabandSystemError: Current DatabandContext context manager wasn't cleaned as expected:
    # actual value=DatabandContext(name='databand_test_context: test_dbnd/trackers/test_async_tracking.py::TestAsyncTracking::test_skip_after_failure (setup)')(139823508534704),
    # expected value=DatabandContext(name='None')(139823509731696)
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        retry=tenacity.retry_if_exception_type(DatabandSystemError),
        reraise=True,
        wait=tenacity.wait_exponential(),
    )
    @patch("dbnd.utils.api_client.ApiClient.api_request")
    def test_skip_after_failure(self, fake_api_request):
        with new_dbnd_context(
            conf={
                "core": {"tracker_raise_on_error": True},
                "databand": {"verbose": True},
            }
        ) as ctx:
            with patch.object(
                TrackingAsyncWebChannel, "_background_worker_skip_processing_callback"
            ) as fake_skip:
                async_store = TrackingStoreThroughChannel.build_with_async_web_channel(
                    ctx
                )
                fake_api_request.side_effect = DatabandWebserverNotReachableError(
                    "fake_message"
                )
                async_store.heartbeat(get_uuid())  # fail here
                async_store.heartbeat(get_uuid())  # skip here
                async_store.flush()
                fake_skip.assert_called_once()

    @patch("dbnd.utils.api_client.ApiClient.api_request")
    def test_no_skip_after_failure(self, fake_api_request):
        with new_dbnd_context(
            conf={
                "core": {"tracker_raise_on_error": False},
                "databand": {"verbose": True},
            }
        ) as ctx:
            with patch.object(
                TrackingAsyncWebChannel, "_background_worker_skip_processing_callback"
            ) as fake_skip:
                async_store = TrackingStoreThroughChannel.build_with_async_web_channel(
                    ctx
                )
                fake_api_request.side_effect = DatabandWebserverNotReachableError(
                    "fake_message"
                )
                async_store.heartbeat(get_uuid())  # fail here
                async_store.heartbeat(get_uuid())  # no skip here
                async_store.flush()
                fake_skip.assert_not_called()

    @patch("dbnd.utils.api_client.ApiClient.api_request")
    def test_flush_without_worker(self, fake_api_request):
        ctx = get_databand_context()
        async_store = TrackingStoreThroughChannel.build_with_async_web_channel(ctx)
        assert not async_store.channel._background_worker.is_alive
        assert async_store.is_ready()
        async_store.flush()
        assert async_store.is_ready()
        async_store.flush()
