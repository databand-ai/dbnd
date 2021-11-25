from dbnd._core.tracking.schemas.base import ApiStrictSchema
from dbnd._vendor.marshmallow import fields, pre_load


class MLAlert(ApiStrictSchema):
    sensitivity = fields.Float()
    look_back = fields.Integer()


class AlertDefsSchema(ApiStrictSchema):
    severity = fields.Str(required=True)
    type = fields.Str(required=True)
    user_metric = fields.Str()
    operator = fields.Str()
    is_str_value = fields.Bool()

    created_at = fields.DateTime()
    scheduled_job_name = fields.Str(attribute="scheduled_job.name")
    source_instance_name = fields.Str(attribute="job.tracking_source.name")
    env = fields.Str(attribute="job.tracking_source.env")
    # TODO_CORE: API: Deprecate airflow_server_info
    airflow_instance_name = fields.Str(attribute="job.tracking_source.name")
    project_id = fields.Int(attribute="job.project_id")
    project_name = fields.Str(attribute="job.project.name")
    alert_on_historical_runs = fields.Bool()

    uid = fields.Str(allow_none=True)
    value = fields.Str(allow_none=True)
    job_id = fields.Int(allow_none=True)
    summary = fields.Str(allow_none=True)
    job_name = fields.Str(attribute="job.name", allow_none=True)
    task_repr = fields.Str(allow_none=True)
    task_name = fields.Str(allow_none=True)
    custom_name = fields.Str(allow_none=True)
    original_uid = fields.Str(allow_none=True)
    advanced_json = fields.Str(allow_none=True)
    scheduled_job_uid = fields.Str(allow_none=True)
    custom_description = fields.Str(allow_none=True)
    ml_alert = fields.Nested(MLAlert, allow_none=True)

    # Used by DatasetMetricAlert/DatasetSlaAdvancedAlert
    seconds_delta = fields.Int(allow_none=True)  # Converts to datetime.timedelta
    dataset_schema_uri = fields.Str(allow_none=True)
    datasets_uids = fields.List(fields.Str(), allow_none=True)

    # Read only value
    affected_datasets = fields.List(fields.Dict(), allow_none=True)

    @pre_load
    def prepere(self, data: dict, **kwargs):
        value = data.get("value", None)
        if value is not None:
            data["value"] = str(data["value"])
        return data
