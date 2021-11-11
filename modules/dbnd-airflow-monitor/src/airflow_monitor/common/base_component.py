from typing import Optional, Type
from uuid import UUID

from airflow_monitor.data_fetcher import get_data_fetcher
from airflow_monitor.shared.base_syncer import BaseMonitorSyncer
from airflow_monitor.tracking_service import get_tracking_service


def start_syncer(
    factory: Type[BaseMonitorSyncer],
    tracking_source_uid: UUID,
    run: Optional[bool] = True,
):
    tracking_service = get_tracking_service(tracking_source_uid=tracking_source_uid)
    monitor_config = tracking_service.get_monitor_configuration()
    data_fetcher = get_data_fetcher(monitor_config)
    syncer = factory(
        config=monitor_config,
        tracking_service=tracking_service,
        data_fetcher=data_fetcher,
    )
    if run:
        syncer.run()
    return syncer
