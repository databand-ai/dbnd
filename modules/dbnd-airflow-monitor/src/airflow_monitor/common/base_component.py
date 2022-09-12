# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional, Type
from uuid import UUID

from airflow_monitor.multiserver.airflow_services_factory import (
    get_airflow_monitor_services_factory,
)
from airflow_monitor.shared.base_syncer import BaseMonitorSyncer


def start_syncer(
    factory: Type[BaseMonitorSyncer],
    tracking_source_uid: UUID,
    run: Optional[bool] = True,
):
    service_factory = get_airflow_monitor_services_factory()
    tracking_service = service_factory.get_tracking_service(
        tracking_source_uid=tracking_source_uid
    )
    monitor_config = tracking_service.get_monitor_configuration()
    data_fetcher = service_factory.get_data_fetcher(monitor_config)
    syncer = factory(
        config=monitor_config,
        tracking_service=tracking_service,
        data_fetcher=data_fetcher,
    )
    if run:
        syncer.run()
    return syncer
