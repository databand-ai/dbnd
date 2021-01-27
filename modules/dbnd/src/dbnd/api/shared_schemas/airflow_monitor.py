import datetime

from typing import Dict, List, Optional

import attr

from dbnd._core.tracking.schemas.base import _ApiCallSchema
from dbnd._vendor.marshmallow import fields, post_load


class AirflowServerInfoSchema(_ApiCallSchema):
    base_url = fields.String()
    external_url = fields.String(allow_none=True)
    airflow_instance_uid = fields.String(allow_none=True)
    airflow_version = fields.String(allow_none=True)
    airflow_export_version = fields.String(allow_none=True)
    airflow_monitor_version = fields.String(allow_none=True)
    dags_path = fields.String(allow_none=True)
    logs_path = fields.String(allow_none=True)
    last_sync_time = fields.DateTime(allow_none=True)
    monitor_status = fields.String(allow_none=True)
    monitor_error_message = fields.String(allow_none=True)
    monitor_start_time = fields.DateTime(allow_none=True)
    synced_from = fields.DateTime(allow_none=True)
    synced_to = fields.DateTime(allow_none=True)
    api_mode = fields.String(allow_none=True)
    sync_interval = fields.Integer(allow_none=True)
    is_sync_enabled = fields.Boolean(allow_none=True)
    fetcher = fields.String(allow_none=True)
    composer_client_id = fields.String(allow_none=True)
    active_dags = fields.Dict(allow_none=True)
    name = fields.String(allow_none=True)
    env = fields.String(allow_none=True)

    @post_load
    def make_object(self, data, **kwargs):
        return AirflowServerInfo(**data)


airflow_server_info_schema = AirflowServerInfoSchema()


@attr.s
class AirflowServerInfo(object):
    base_url = attr.ib()  # type: str
    external_url = attr.ib(default=None)  # type: Optional[str]
    airflow_instance_uid = attr.ib(default=None)  # type: str
    airflow_version = attr.ib(default=None)  # type: Optional[str]
    airflow_export_version = attr.ib(default=None)  # type: Optional[str]
    airflow_monitor_version = attr.ib(default=None)  # type: Optional[str]
    dags_path = attr.ib(default=None)  # type: Optional[str]
    logs_path = attr.ib(default=None)  # type: Optional[str]
    last_sync_time = attr.ib(default=None)  # type: Optional[datetime.datetime]
    monitor_status = attr.ib(default=None)  # type: Optional[str]
    monitor_error_message = attr.ib(default=None)  # type: Optional[str]
    monitor_start_time = attr.ib(default=None)  # type: Optional[datetime.datetime]
    synced_from = attr.ib(default=None)  # type: Optional[datetime.datetime]
    synced_to = attr.ib(default=None)  # type: Optional[datetime.datetime]
    api_mode = attr.ib(default="rbac")  # type: Optional[str]
    sync_interval = attr.ib(default=None)  # type: Optional[int]
    is_sync_enabled = attr.ib(default=None)  # type: Optional[bool]
    fetcher = attr.ib(default=None)  # type: Optional[str]
    composer_client_id = attr.ib(default=None)  # type: Optional[str]
    active_dags = attr.ib(default=None)  # type: Dict[str, List[str]]
    name = attr.ib(default=None)  # type: Optional[str]
    env = attr.ib(default=None)  # type: Optional[str]
