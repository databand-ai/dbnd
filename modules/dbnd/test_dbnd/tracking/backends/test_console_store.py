# © Copyright Databand.ai, an IBM Company 2022
import logging

import mock
import pytest

from dbnd import dbnd_config, dbnd_tracking, set_verbose, task
from dbnd._core.settings import CoreConfig


@pytest.fixture
def mock_logger():
    with mock.patch(
        "dbnd._core.tracking.script_tracking_manager.logger"
    ) as mock_logger:
        yield mock_logger


@task
def task_test_logging():
    logging.info("this is ground control to major tom")


def tracking_scenario__test_logging():
    with dbnd_tracking():
        task_test_logging()


def test_af_tracking_mode_tracker_url_logging(mock_logger):
    set_verbose()
    with dbnd_config(
        {CoreConfig.tracker: ["console"], CoreConfig.databand_url: "http://databand.ai"}
    ):
        tracking_scenario__test_logging()

    assert "DBND: Your run is tracked by DBND" in mock_logger._calls_repr()
    assert "http://databand.ai" in mock_logger._calls_repr()
