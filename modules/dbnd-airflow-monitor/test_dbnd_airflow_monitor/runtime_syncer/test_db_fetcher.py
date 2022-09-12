# © Copyright Databand.ai, an IBM Company 2022

import pytest

from mock import MagicMock

from airflow_monitor.data_fetcher.base_data_fetcher import AirflowDataFetcher
from airflow_monitor.shared.decorators import decorate_fetcher


class TestFetcher(AirflowDataFetcher):
    pass


def test_db_fetcher_retries():
    class TestException(Exception):
        pass

    test_fetcher = TestFetcher(MagicMock())
    func_mock = MagicMock(
        side_effect=TestException(), __name__="get_airflow_dagruns_to_sync"
    )
    test_fetcher.get_airflow_dagruns_to_sync = func_mock
    decorated_fetcher = decorate_fetcher(test_fetcher, "some label")
    with pytest.raises(TestException):
        decorated_fetcher.get_airflow_dagruns_to_sync()
    # it should be called more than once
    assert func_mock.call_count == 3
