# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import ClassVar, Union
from uuid import UUID

from prometheus_client import Summary

from dbnd._core.utils.trace import new_tracing_id
from dbnd_monitor.base_integration_config import BaseIntegrationConfig
from dbnd_monitor.base_tracking_service import BaseTrackingService
from dbnd_monitor.reporting_service import ReportingService
from dbnd_monitor.utils.apm import transaction_scope
from dbnd_monitor.utils.prometheus_tools import sync_once_time


logger = logging.getLogger(__name__)


class BaseComponent:
    """
    BaseComponent is a component responsible for syncing data from given server to tracking service
    """

    SYNCER_TYPE: ClassVar[str]
    config: BaseIntegrationConfig
    tracking_service: BaseTrackingService
    reporting_service: ReportingService
    data_fetcher: object
    sleep_interval: int

    def __init__(
        self,
        config: BaseIntegrationConfig,
        tracking_service: BaseTrackingService,
        reporting_service: ReportingService,
        data_fetcher: object = None,
    ):
        self.config = config
        self.tracking_service = tracking_service
        self.reporting_service = reporting_service
        self.data_fetcher: object = data_fetcher

    @property
    def sleep_interval(self):
        return self.config.sync_interval

    def refresh_config(self, config: BaseIntegrationConfig):
        self.config = config
        if (
            self.config.log_level
            and logging.getLevelName(self.config.log_level) != logging.root.level
        ):
            logging.root.setLevel(self.config.log_level)

    def sync_once(self):

        logger.info(
            "Starting sync_once on tracking source uid: %s, syncer: %s",
            self.server_id,
            self.identifier,
        )

        with new_tracing_id(), self._time_sync_once(), self.error_handler(
            self, "sync_once"
        ), transaction_scope(f"{self.config.source_type}.{self.SYNCER_TYPE}.sync_once"):
            result = self._sync_once()

            logger.info(
                "Finished sync_once on tracking source uid: %s, syncer: %s",
                self.server_id,
                self.identifier,
            )
            return result

    def _time_sync_once(self) -> Summary:
        return sync_once_time.labels(
            integration=self.config.uid,
            syncer=self.SYNCER_TYPE,
            fetcher=self.config.fetcher_type,
        ).time()

    def _sync_once(self):
        raise NotImplementedError()

    def __str__(self):
        return f"{self.__class__.__name__}({self.config.source_name}|{self.config.uid})"

    def report_sync_metrics(self, is_success: bool) -> None:
        """
        reports sync metrics when required.
        called from capture_component_exception()
        """

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def identifier(self) -> str:
        return self.SYNCER_TYPE

    @property
    def server_id(self) -> Union[str, UUID]:
        if self.config.tracking_source_uid:
            return self.config.tracking_source_uid

        return self.tracking_service.tracking_source_uid

    @property
    def external_id(self) -> str:
        return self.config.integration_config.get("external_id", "None")

    @property
    def error_handler(self):
        from dbnd_monitor.error_handling.error_handler import (  # noqa: Cyclic import
            capture_component_exception,
            capture_component_exception_as_component_error,
        )

        if self.config.component_error_support:
            return capture_component_exception_as_component_error
        return capture_component_exception

    def report_errors(self):
        if self.config.component_error_support:
            self.reporting_service.report_errors_dto(
                self.config.uid, self.external_id, self.name
            )
