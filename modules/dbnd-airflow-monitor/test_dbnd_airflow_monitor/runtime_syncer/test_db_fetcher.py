# © Copyright Databand.ai, an IBM Company 2022

import pytest

from mock import MagicMock

from airflow_monitor.data_fetcher.base_data_fetcher import AirflowDataFetcher


class TestFetcher(AirflowDataFetcher):
    pass


def test_db_fetcher_retries():
    class TestException(Exception):
        pass

    func_mock = MagicMock(
        side_effect=TestException(), __name__="get_airflow_dagruns_to_sync"
    )
    TestFetcher.get_airflow_dagruns_to_sync = func_mock
    test_fetcher = TestFetcher(MagicMock())
    decorated_fetcher = test_fetcher
    with pytest.raises(TestException):
        decorated_fetcher.get_airflow_dagruns_to_sync()
    # it should be called more than once
    assert func_mock.call_count == 3
