# © Copyright Databand.ai, an IBM Company 2024

import logging

from datetime import timedelta
from sys import getsizeof
from typing import List

from dbnd._core.utils.timezone import utcnow
from dbnd.utils.api_client import ApiClient
from dbnd_monitor.utils import _get_api_client


def get_logs_buffer_size_in_kb(buffer: List[str]):
    return getsizeof(buffer) / 1024


def get_time_from_last_log_send(last_log_time_send):
    return utcnow() - last_log_time_send


class RemoteMonitorLogHandler(logging.Handler):
    def __init__(
        self,
        monitor_type: str,
        syncer_name: str,
        maximum_logs_buffer_size_in_kb: int,
        max_time_delta_to_send_monitor_logs_in_minutes: int,
        secure: bool = True,
    ):
        super().__init__()

        self._api_client: ApiClient = _get_api_client()
        self.monitor_type = monitor_type
        self.syncer_name = syncer_name
        self.maximum_logs_buffer_size_in_kb = maximum_logs_buffer_size_in_kb
        self.max_time_delta_to_send_monitor_logs_in_minutes = (
            max_time_delta_to_send_monitor_logs_in_minutes
        )
        self.secure = secure
        self.send_log_interval = timedelta(
            minutes=self.max_time_delta_to_send_monitor_logs_in_minutes
        )
        self.logs_buffer = []

        self._reset_log_handler()

    def _reset_log_handler(self):
        self.logs_buffer = []
        self.last_log_sent_time = utcnow()

    def _should_send_log_to_gsc_bucket(self):
        current_size_of_logs_buffer = get_logs_buffer_size_in_kb(self.logs_buffer)
        time_from_last_log_send = get_time_from_last_log_send(self.last_log_sent_time)

        return (
            current_size_of_logs_buffer > self.maximum_logs_buffer_size_in_kb
            or time_from_last_log_send >= self.send_log_interval
        )

    def _send_log_to_gsc_bucket(self):
        result = self._api_client.api_request(
            endpoint=f"integrations/{self.monitor_type}/{self.syncer_name}/report-monitor-logs",
            method="POST",
            data=self.logs_buffer,
        )
        return result

    def emit(self, record: logging.LogRecord):
        log_entry = self.format(record)
        self.logs_buffer.append(log_entry)
        if self._should_send_log_to_gsc_bucket():
            self._send_log_to_gsc_bucket()
            self._reset_log_handler()
