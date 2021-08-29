from airflow_monitor.tracking_service.error_aggregator import ErrorAggregator

from . import random_text


def test_01_same_reporter():
    e = ErrorAggregator()

    def reporter():
        pass

    assert not e.report(reporter, None).should_update

    msg1 = random_text()
    res = e.report(reporter, msg1)

    assert res.should_update
    assert msg1 in res.message

    msg2 = random_text()
    res = e.report(reporter, msg2)

    assert res.should_update
    assert msg1 not in res.message
    assert msg2 in res.message

    res = e.report(reporter, None)
    assert res.should_update
    assert res.message is None

    res = e.report(reporter, None)
    assert not res.should_update


def test_02_different_reporters():
    e = ErrorAggregator()

    def reporter1():
        pass

    def reporter2():
        pass

    msg1 = random_text()
    res = e.report(reporter1, msg1)
    assert res.should_update
    assert msg1 in res.message

    msg2 = random_text()
    res = e.report(reporter2, msg2)

    assert res.should_update
    assert msg1 in res.message
    assert msg2 in res.message

    res = e.report(reporter1, None)
    assert res.should_update
    assert msg1 not in res.message
    assert msg2 in res.message

    res = e.report(reporter2, None)
    assert res.should_update
    assert res.message is None

    res = e.report(reporter1, None)
    assert not res.should_update


def test_03_ref_eviction():
    e = ErrorAggregator()

    msg1 = random_text()

    def reporter1():
        pass

    def reporter2():
        pass

    def reporter3():
        pass

    res = e.report(reporter1, msg1)
    assert res.should_update
    assert msg1 in res.message

    del reporter1
    msg2 = random_text()
    res = e.report(reporter2, msg2)
    assert res.should_update
    assert msg1 not in res.message
    assert msg2 in res.message

    del reporter2
    res = e.report(reporter3, None)
    assert res.should_update
    assert res.message is None

    res = e.report(reporter3, None)
    assert not res.should_update
