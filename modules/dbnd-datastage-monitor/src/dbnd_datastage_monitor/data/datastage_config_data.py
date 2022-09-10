# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional

import attr

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from dbnd._core.tracking.schemas.base import ApiStrictSchema
from dbnd._core.utils.basics.nothing import NOTHING
from dbnd._vendor.marshmallow import fields


class DataStageUpdateMonitorStateRequestSchema(ApiStrictSchema):
    monitor_status = fields.String(required=False, allow_none=True)
    monitor_error_message = fields.String(required=False, allow_none=True)
    last_sync_time = fields.DateTime(required=False, allow_none=True)


@attr.s
class DataStageServerConfig(BaseServerConfig):
    project_id = attr.ib(default=None)  # type: str
    api_key = attr.ib(default=None)  # type: str
    runs_bulk_size = attr.ib(default=10)  # type: int
    page_size = attr.ib(default=200)  # type: int
    fetching_interval_in_minutes = attr.ib(default=30)  # type: int
    number_of_fetching_threads = attr.ib(default=1)  # type: int
    datastage_runs_syncer_enabled = attr.ib(default=True)  # type: bool

    @classmethod
    def create(
        cls, server_config: dict, monitor_config: Optional[BaseMonitorConfig] = None
    ):
        monitor_instance_config = server_config.get("monitor_config") or {}

        conf = cls(
            source_type="datastage",
            source_name=server_config["source_name"],
            tracking_source_uid=server_config["tracking_source_uid"],
            is_sync_enabled=server_config["is_sync_enabled"],
            runs_bulk_size=monitor_instance_config["runs_bulk_size"],
            page_size=monitor_instance_config["page_size"],
            fetching_interval_in_minutes=monitor_instance_config[
                "fetching_interval_in_minutes"
            ],
            number_of_fetching_threads=monitor_instance_config[
                "number_of_fetching_threads"
            ],
            project_id=server_config["project_id"],
            api_key=server_config["api_key"],
            sync_interval=monitor_instance_config["sync_interval"],
            datastage_runs_syncer_enabled=monitor_instance_config[
                "datastage_runs_syncer_enabled"
            ],
        )
        return conf


@attr.s
class DataStageMonitorState:
    monitor_status = attr.ib(default=NOTHING)
    monitor_error_message = attr.ib(default=NOTHING)
    last_sync_time = attr.ib(default=NOTHING)

    def as_dict(self):
        # Return only non NOTHING values (that did not change), None values are OK
        return attr.asdict(
            self, filter=lambda attr_name, attr_value: attr_value != NOTHING
        )


class DataStageMonitorConfig(BaseMonitorConfig):
    _conf__task_family = "datastage_monitor"
