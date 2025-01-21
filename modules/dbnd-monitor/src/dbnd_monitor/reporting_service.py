# © Copyright Databand.ai, an IBM Company 2022

from collections import defaultdict
from typing import DefaultDict, Optional, Tuple, Type, Union
from uuid import UUID

import dbnd_monitor

from dbnd._core.utils.timezone import utcnow
from dbnd.utils.api_client import ApiClient
from dbnd_monitor.error_handling.component_error import ComponentError, ReportErrorsDTO
from dbnd_monitor.error_handling.error_aggregator import (
    ComponentErrorAggregator,
    ErrorAggregator,
    ErrorAggregatorResult,
)
from dbnd_monitor.utils.api_client import _get_api_client


K = Union[UUID, Tuple[UUID, str, str]]  # K for Key
A = Union[ErrorAggregator, ComponentErrorAggregator]  # A for Aggregator


class ReportingService:
    def __init__(
        self, monitor_type: str, aggregator: Type[A] = ErrorAggregator
    ) -> None:
        self.monitor_type: str = monitor_type
        self._api_client: ApiClient = _get_api_client()
        self._error_aggregators: DefaultDict[K, A] = defaultdict(aggregator)

    def report_monitor_time_data(
        self, integration_uid: UUID, synced_new_data: bool = False
    ):
        current_time = utcnow().isoformat()
        data = {"last_sync_time": current_time}
        if synced_new_data:
            data["last_update_time"] = current_time

        self._api_client.api_request(
            endpoint=f"integrations/{integration_uid}/monitor_time_data?type={self.monitor_type}",
            method="PATCH",
            data=data,
        )

    def report_metadata(self, integration_uid: UUID, metadata: dict):
        self._api_client.api_request(
            endpoint=f"integrations/{integration_uid}/metadata?type={self.monitor_type}",
            method="PATCH",
            data={"monitor_metadata": metadata},
        )

    def report_error(
        self, integration_uid: UUID, full_function_name: str, err_message: Optional[str]
    ):
        res = self._error_aggregators[integration_uid].report(
            full_function_name, err_message
        )
        self._report_error(integration_uid, res)

    def report_component_error(
        self,
        integration_uid: UUID,
        external_id: str,
        component: str,
        component_error: ComponentError,
    ):
        key = (integration_uid, external_id, component)
        self._error_aggregators[key].report_component_error(component_error)

    def report_errors_dto(
        self, integration_uid: UUID, external_id: str, component: str
    ):
        key = (integration_uid, external_id, component)
        errors, should_update = self._error_aggregators[key].report()

        if not should_update:
            return

        errors_dto = ReportErrorsDTO(
            tracking_source_uid=str(integration_uid),
            external_id=external_id,
            component=component,
            errors=errors,
        )

        self._api_client.api_request(
            endpoint=f"integrations/{integration_uid}/error?type={self.monitor_type}",
            method="PATCH",
            data=errors_dto.dump(),
        )

    def clean_error_message(self, integration_uid: UUID):
        self._report_error(
            integration_uid, ErrorAggregatorResult(None, should_update=True)
        )

    def _report_error(self, integration_uid, res: ErrorAggregatorResult):
        if not res.should_update:
            return

        self._api_client.api_request(
            endpoint=f"integrations/{integration_uid}/error?type={self.monitor_type}",
            method="PATCH",
            data={"monitor_error_message": res.message},
        )

    def report_exception_to_web_server(self, exception: str):
        data = {
            "dbnd_version": dbnd_monitor.__version__,
            "source": f"{self.monitor_type}_monitor",
            "stack_trace": exception,
            "timestamp": utcnow().isoformat(),
        }
        return self._api_client.api_request(
            endpoint="log_exception", method="POST", data=data
        )
