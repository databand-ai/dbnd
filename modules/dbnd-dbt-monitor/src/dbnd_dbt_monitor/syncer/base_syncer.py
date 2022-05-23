from typing import Optional, Type
from uuid import UUID

from dbnd_dbt_monitor.data.dbt_config_data import DbtServerConfig
from dbnd_dbt_monitor.fetcher.fetcher_decorators import get_data_fetcher
from dbnd_dbt_monitor.tracking_service.dbnd_dbt_tracking_service import (
    DbndDbtTrackingService,
)
from dbnd_dbt_monitor.tracking_service.tracking_service_decorators import (
    get_tracking_service,
)

from airflow_monitor.shared.base_syncer import BaseMonitorSyncer


def start_syncer(
    factory: Type[BaseMonitorSyncer],
    tracking_source_uid: UUID,
    run: Optional[bool] = True,
):
    tracking_service: DbndDbtTrackingService = get_tracking_service(
        tracking_source_uid=tracking_source_uid
    )
    monitor_config: DbtServerConfig = tracking_service.get_monitor_configuration()
    data_fetcher = get_data_fetcher(monitor_config)
    syncer: BaseMonitorSyncer = factory(
        config=monitor_config,
        tracking_service=tracking_service,
        data_fetcher=data_fetcher,
    )
    if run:
        syncer.run()
    return syncer
