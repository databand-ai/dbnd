import pytest

from dbnd import new_dbnd_context


def first_store(tracking_store):
    """
    Returns the first store, otherwise None.
    """
    if tracking_store._stores:
        return next(iter(tracking_store._stores.values()))

    return None


class TestProtobufImportability(object):
    def test_init_protoweb_channel(self):
        # testing env should not include protobuf package
        # otherwise this test is useless
        with pytest.raises(ImportError):
            from google import protobuf

        with new_dbnd_context(
            conf={
                "core": {
                    "databand_url": "http://fake-url.dbnd.local:8080",
                    "tracker": ["api"],
                    "tracker_api": "proto",
                    "tracker_raise_on_error": True,
                    "allow_vendored_package": True,
                }
            }
        ) as dc:
            ts = first_store(dc.tracking_store)
            assert ts.__class__.__name__ == "TrackingStoreThroughChannel", ts
            assert (
                ts.channel.__class__.__name__ == "TrackingProtoWebChannel"
            ), ts.channel
            # an extra check that protobuf is available:
            from google import protobuf
