# © Copyright Databand.ai, an IBM Company 2024

import logging
import uuid

from datetime import datetime, timedelta, timezone

import pytest

from mock import patch
from pytest import fixture

from dbnd_monitor.remote_monitor_log_handler import (
    RemoteMonitorLogHandler,
    get_time_from_last_log_send,
)


@patch(
    "dbnd_monitor.remote_monitor_log_handler.utcnow",
    return_value=datetime(2024, 1, 16, 10, 0, 59, tzinfo=timezone.utc),
)
def test_get_time_from_last_log_send(utcnow_mock):
    last_log_time_send = datetime(2024, 1, 16, 10, 0, 0, tzinfo=timezone.utc)
    time_between_log_send = get_time_from_last_log_send(last_log_time_send)
    assert time_between_log_send == timedelta(seconds=59)


@fixture()
def remote_monitor_log_handler():
    return RemoteMonitorLogHandler("airflow", uuid.uuid4(), 50, 2)


def test_reset_log_handler(remote_monitor_log_handler):
    remote_monitor_log_handler.logs_buffer.append("test_log")
    assert len(remote_monitor_log_handler.logs_buffer) > 0
    remote_monitor_log_handler._reset_log_handler()
    assert len(remote_monitor_log_handler.logs_buffer) == 0


@pytest.mark.parametrize(
    "minutes_from_last_log_send, current_size_of_logs_buffer, should_send_next_log, ",
    [(10, 40, True), (1, 40, False), (2, 80, True), (2, 50, True), (0, 0, False)],
)
@patch("dbnd_monitor.remote_monitor_log_handler.get_time_from_last_log_send")
@patch("dbnd_monitor.remote_monitor_log_handler.get_logs_buffer_size_in_kb")
def test_should_send_log_to_gcs_bucket(
    mock_get_logs_buffer_size,
    mock_get_time_from_last_log_send,
    minutes_from_last_log_send,
    current_size_of_logs_buffer,
    should_send_next_log,
    remote_monitor_log_handler,
):

    mock_get_logs_buffer_size.return_value = current_size_of_logs_buffer
    mock_get_time_from_last_log_send.return_value = timedelta(
        minutes=minutes_from_last_log_send
    )
    result = remote_monitor_log_handler._should_send_log_to_gsc_bucket()

    mock_get_logs_buffer_size.assert_called_once()
    mock_get_time_from_last_log_send.assert_called_once()

    assert result == should_send_next_log


@patch("dbnd.utils.api_client.ApiClient.api_request")
def test_send_logs_to_gcs_bucket(mock_api_request, remote_monitor_log_handler):
    mock_api_request.return_value({"is_success": True})

    remote_monitor_log_handler._send_log_to_gsc_bucket()

    mock_api_request.assert_called_once_with(
        endpoint=f"integrations/{remote_monitor_log_handler.monitor_type}/{remote_monitor_log_handler.syncer_name}/report-monitor-logs",
        method="POST",
        data=remote_monitor_log_handler.logs_buffer,
    )


@pytest.mark.parametrize("should_send_logs", [True, False])
@patch(
    "dbnd_monitor.remote_monitor_log_handler.RemoteMonitorLogHandler._reset_log_handler"
)
@patch(
    "dbnd_monitor.remote_monitor_log_handler.RemoteMonitorLogHandler._send_log_to_gsc_bucket"
)
@patch(
    "dbnd_monitor.remote_monitor_log_handler.RemoteMonitorLogHandler._should_send_log_to_gsc_bucket"
)
def test_emit(
    mock_should_send_logs,
    mock_send_logs_to_gsc,
    mock_reset_logs_handler,
    should_send_logs,
    remote_monitor_log_handler,
):
    mock_send_logs_to_gsc.return_value = None

    if should_send_logs:
        mock_should_send_logs.return_value = should_send_logs

        remote_monitor_log_handler.emit(
            logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="test",
                lineno=1,
                args=None,
                exc_info=None,
                msg="test_log",
            )
        )

        mock_should_send_logs.assert_called_once()

        mock_send_logs_to_gsc.assert_called_once()
        mock_reset_logs_handler.assert_called_once()
    if not should_send_logs:
        mock_should_send_logs.return_value = should_send_logs

        remote_monitor_log_handler.emit(
            logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="test",
                lineno=1,
                args=None,
                exc_info=None,
                msg="test_log",
            )
        )

        mock_should_send_logs.assert_called_once()
        mock_send_logs_to_gsc.assert_not_called()
        mock_reset_logs_handler.assert_not_called()
