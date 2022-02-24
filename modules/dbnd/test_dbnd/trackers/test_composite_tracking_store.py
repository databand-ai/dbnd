import pytest

from mock import Mock

from dbnd._core.errors.base import DatabandWebserverNotReachableError
from dbnd._core.tracking.backends import tracking_store_composite
from dbnd._core.tracking.backends.tracking_store_composite import (
    CompositeTrackingStore,
    try_run_handler,
)


class TestCompositeTrackingStore(object):
    @pytest.fixture()
    def store(self):
        store = Mock()
        store.panic_fails = Mock(side_effect=DatabandWebserverNotReachableError("msg"))
        store.non_panic_fails = Mock(side_effect=Exception())
        store.success = Mock(return_value=True)
        store.fails_on_first = Mock(side_effect=[Exception(), True])
        store.is_ready = Mock(return_value=True)
        return store

    def test_try_run_handler_happy_flow(self, store):
        try_run_handler(100, store, "success", {})
        assert store.success.call_count == 1

    def test_try_run_handler_panic_fails(self, store):
        with pytest.raises(DatabandWebserverNotReachableError):
            try_run_handler(10, store, "panic_fails", {})

        assert store.panic_fails.call_count == 10

    def test_try_run_handler_fails_on_first(self, store):
        try_run_handler(10, store, "fails_on_first", {})
        assert store.fails_on_first.call_count == 2

    def get_composite_tracking_store(self, store):
        return CompositeTrackingStore({"mock_store": store}, max_retires=2)

    def test_invoke_happy_flow(self, store):
        composite_store = self.get_composite_tracking_store(store)
        assert composite_store._invoke("success", {})
        assert store.success.call_count == 1

    def test_invoke_failed_non_state_handler_no_tracking(self, monkeypatch, store):
        composite_store = self.get_composite_tracking_store(store)
        monkeypatch.setattr(tracking_store_composite, "is_state_call", lambda x: False)
        monkeypatch.setattr(tracking_store_composite, "in_tracking_run", lambda: False)
        composite_store._invoke("non_panic_fails", {})
        assert store.non_panic_fails.call_count == 1
        composite_store.has_tracking_store("mock_store")

    def test_invoke_failed_state_handler_no_tracking(self, monkeypatch, store):
        composite_store = self.get_composite_tracking_store(store)
        monkeypatch.setattr(tracking_store_composite, "is_state_call", lambda x: True)
        monkeypatch.setattr(tracking_store_composite, "in_tracking_run", lambda: False)
        composite_store._invoke("non_panic_fails", {})
        assert store.non_panic_fails.call_count > 1
        composite_store.has_tracking_store("mock_store")

    def test_invoke_failed_non_state_handler_in_tracking(self, monkeypatch, store):
        composite_store = self.get_composite_tracking_store(store)
        monkeypatch.setattr(tracking_store_composite, "is_state_call", lambda x: False)
        monkeypatch.setattr(tracking_store_composite, "in_tracking_run", lambda: True)
        composite_store._invoke("non_panic_fails", {})
        assert store.non_panic_fails.call_count == 1
        composite_store.has_tracking_store("mock_store")

    def test_invoke_failed_state_handler_in_tracking(self, monkeypatch, store):
        composite_store = self.get_composite_tracking_store(store)
        monkeypatch.setattr(tracking_store_composite, "is_state_call", lambda x: True)
        monkeypatch.setattr(tracking_store_composite, "in_tracking_run", lambda: True)
        composite_store._invoke("non_panic_fails", {})
        assert store.non_panic_fails.call_count > 1
        assert not composite_store.has_tracking_store("mock_store")

    def test_invoke_raise_on_panic_error(self, store):
        composite_store = self.get_composite_tracking_store(store)
        with pytest.raises(DatabandWebserverNotReachableError):
            composite_store._invoke("panic_fails", {})

    def test_is_ready(self, store):
        composite_store = self.get_composite_tracking_store(store)
        assert composite_store.is_ready()
