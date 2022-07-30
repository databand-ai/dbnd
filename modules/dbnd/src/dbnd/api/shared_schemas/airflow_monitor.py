# © Copyright Databand.ai, an IBM Company 2022

import datetime

from typing import Dict, Optional

import attr

from dbnd._core.errors.base import DatabandBadRequest
from dbnd._core.tracking.schemas.base import ApiStrictSchema
from dbnd._vendor.marshmallow import ValidationError, fields, post_load, validate


class MonitorConfigSchema(ApiStrictSchema):
    include_sources = fields.Boolean(required=False)
    dag_run_bulk_size = fields.Integer(required=False)
    start_time_window = fields.Integer(required=False)
    log_bytes_from_head = fields.Integer(required=False)
    log_bytes_from_end = fields.Integer(required=False)
    use_async_tracking = fields.Boolean(required=False)


class AirflowServerInfoSchema(ApiStrictSchema):
    airflow_version = fields.String(allow_none=True)
    airflow_export_version = fields.String(allow_none=True)
    airflow_monitor_version = fields.String(allow_none=True)
    last_sync_time = fields.DateTime(allow_none=True)
    monitor_error_message = fields.String(allow_none=True)
    synced_to = fields.DateTime(allow_none=True)
    api_mode = fields.String(allow_none=True)
    sync_interval = fields.Integer(allow_none=True)
    is_sync_enabled = fields.Boolean(allow_none=True)
    system_alert_definitions = fields.Dict()
    fetcher = fields.String(allow_none=True)
    composer_client_id = fields.String(allow_none=True)
    dag_ids = fields.String(allow_none=True)

    base_url = fields.String()
    external_url = fields.String(allow_none=True)
    source_instance_uid = fields.String(allow_none=True)
    tracking_source_uid = fields.String()
    airflow_instance_uid = fields.String(allow_none=True)  # TODO_API: deprecate
    name = fields.String(allow_none=True)
    env = fields.String(allow_none=True)
    monitor_status = fields.String(allow_none=True)
    monitor_config = fields.Nested(MonitorConfigSchema, allow_none=True)
    airflow_environment = fields.String(allow_none=True)
    last_seen_dag_run_id = fields.Integer(allow_none=True)
    last_seen_log_id = fields.Integer(allow_none=True)

    @post_load
    def make_object(self, data, **kwargs):
        return AirflowServerInfo(**data)


airflow_server_info_schema = AirflowServerInfoSchema()


def is_url_valid(url):
    return validate.URL(schemes={"http", "https"}, require_tld=False)(url)


def url_validation(obj, attribute, value):
    if value is not None and value:
        try:
            return is_url_valid(value)
        except ValidationError:
            raise DatabandBadRequest(
                "Could not edit airflow syncer because {} url {} was not a valid url".format(
                    attribute.name, value
                )
            )


@attr.s
class AirflowServerInfo(object):
    base_url = attr.ib(validator=url_validation)  # type: str
    monitor_status = attr.ib(default=None)  # type: Optional[str]
    name = attr.ib(default=None)  # type: Optional[str]
    env = attr.ib(default=None)  # type: Optional[str]
    external_url = attr.ib(
        default=None, validator=url_validation
    )  # type: Optional[str]
    source_instance_uid = attr.ib(default=None)  # type: str
    tracking_source_uid = attr.ib(default=None)  # type: str
    airflow_instance_uid = attr.ib(default=None)  # type: str #TODO_API: derperate
    airflow_version = attr.ib(default=None)  # type: Optional[str]
    airflow_export_version = attr.ib(default=None)  # type: Optional[str]
    airflow_monitor_version = attr.ib(default=None)  # type: Optional[str]
    last_sync_time = attr.ib(default=None)  # type: Optional[datetime.datetime]
    monitor_error_message = attr.ib(default=None)  # type: Optional[str]
    synced_to = attr.ib(default=None)  # type: Optional[datetime.datetime]
    api_mode = attr.ib(default="rbac")  # type: Optional[str]
    is_sync_enabled = attr.ib(default=None)  # type: Optional[bool]
    system_alert_definitions = attr.ib(default=None)  # type: Optional[Dict]
    fetcher = attr.ib(default=None)  # type: Optional[str]
    composer_client_id = attr.ib(default=None)  # type: Optional[str]
    dag_ids = attr.ib(default=None)  # type: Optional[str]
    monitor_config = attr.ib(default=None)  # type: Optional[Dict]
    airflow_environment = attr.ib(default=None)  # type: Optional[str]
    last_seen_dag_run_id = attr.ib(default=None)  # type: Optional[int]
    last_seen_log_id = attr.ib(default=None)  # type: Optional[int]
