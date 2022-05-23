from typing import Optional

import attr

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from dbnd._core.tracking.schemas.base import ApiStrictSchema
from dbnd._core.utils.basics.nothing import NOTHING
from dbnd._vendor.marshmallow import fields


class DbtUpdateMonitorStateRequestSchema(ApiStrictSchema):
    monitor_status = fields.String(required=False, allow_none=True)
    monitor_error_message = fields.String(required=False, allow_none=True)
    last_sync_time = fields.DateTime(required=False, allow_none=True)


@attr.s
class DbtServerConfig(BaseServerConfig):
    account_id = attr.ib(default=None)  # type: int
    api_token = attr.ib(default=None)  # type: str
    job_id = attr.ib(default=None)  # type: int
    runs_bulk_size = attr.ib(default=10)  # type: int
    dbt_runs_syncer_enabled = attr.ib(default=True)  # type: bool

    @classmethod
    def create(
        cls, server_config: dict, monitor_config: Optional[BaseMonitorConfig] = None
    ):
        monitor_instance_config = server_config.get("monitor_config") or {}

        conf = cls(
            source_type="dbt",
            source_name=server_config["source_name"],
            tracking_source_uid=server_config["tracking_source_uid"],
            is_sync_enabled=server_config["is_sync_enabled"],
            runs_bulk_size=monitor_instance_config["runs_bulk_size"],
            account_id=server_config["account_id"],
            api_token=server_config["api_token"],
            sync_interval=monitor_instance_config["sync_interval"],
            job_id=monitor_instance_config.get("job_id"),
            dbt_runs_syncer_enabled=monitor_instance_config["dbt_runs_syncer_enabled"],
        )
        return conf


@attr.s
class DbtMonitorState:
    monitor_status = attr.ib(default=NOTHING)
    monitor_error_message = attr.ib(default=NOTHING)
    last_sync_time = attr.ib(default=NOTHING)

    def as_dict(self):
        # Return only non NOTHING values (that did not change), None values are OK
        return attr.asdict(
            self, filter=lambda attr_name, attr_value: attr_value != NOTHING
        )


class DbtMonitorConfig(BaseMonitorConfig):
    _conf__task_family = "dbt_monitor"
