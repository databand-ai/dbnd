from dbnd._core.tracking.schemas.base import ApiObjectSchema
from dbnd._vendor.marshmallow import fields


class GetRunningDagRunsResponseSchema(ApiObjectSchema):
    dag_run_ids = fields.List(fields.Integer())
    last_seen_dag_run_id = fields.Integer(allow_none=True)
    last_seen_log_id = fields.Integer(allow_none=True)


class UpdateLastSeenValuesRequestSchema(ApiObjectSchema):
    last_seen_dag_run_id = fields.Integer()
    last_seen_log_id = fields.Integer()


class UpdateMonitorStateRequestSchema(ApiObjectSchema):
    airflow_version = fields.String(required=False, allow_none=True)
    airflow_export_version = fields.String(required=False, allow_none=True)
    airflow_monitor_version = fields.String(required=False, allow_none=True)
    dags_path = fields.String(required=False, allow_none=True)
    logs_path = fields.String(required=False, allow_none=True)
    monitor_status = fields.String(required=False, allow_none=True)
    monitor_error_message = fields.String(required=False, allow_none=True)
    monitor_start_time = fields.DateTime(required=False, allow_none=True)
    airflow_instance_uid = fields.UUID(required=False, allow_none=True)


class GetAllDagRunsRequestSchema(ApiObjectSchema):
    min_start_time = fields.DateTime(allow_none=True)
    dag_ids = fields.String(allow_none=True)


class TaskSchema(ApiObjectSchema):
    upstream_task_ids = fields.List(fields.String())
    downstream_task_ids = fields.List(fields.String())
    task_type = fields.String()
    task_source_code = fields.String(allow_none=True)
    task_source_hash = fields.String(allow_none=True)
    task_module_code = fields.String(allow_none=True)
    module_source_hash = fields.String(allow_none=True)
    dag_id = fields.String()
    task_id = fields.String()
    retries = fields.Integer()
    command = fields.String(allow_none=True)
    task_args = fields.Dict()


class DagSchema(ApiObjectSchema):
    description = fields.String()
    root_task_ids = fields.List(fields.String())
    tasks = fields.Nested(TaskSchema, many=True)
    owner = fields.String()
    dag_id = fields.String()
    schedule_interval = fields.String()
    catchup = fields.Boolean()
    start_date = fields.DateTime(allow_none=True)
    end_date = fields.DateTime(allow_none=True)
    is_committed = fields.Boolean()
    git_commit = fields.String()
    dag_folder = fields.String()
    hostname = fields.String()
    source_code = fields.String(allow_none=True)
    module_source_hash = fields.String(allow_none=True)
    is_subdag = fields.Boolean()
    tags = fields.List(fields.String(), allow_none=True)
    task_type = fields.String()
    task_args = fields.Dict()
    is_active = fields.Boolean(allow_none=True)
    is_paused = fields.Boolean(allow_none=True)


class DagRunSchema(ApiObjectSchema):
    dag_id = fields.String()
    run_id = fields.String(required=False)
    dagrun_id = fields.Integer()
    start_date = fields.DateTime(allow_none=True)
    state = fields.String()
    end_date = fields.DateTime(allow_none=True)
    execution_date = fields.DateTime()
    task_args = fields.Dict()


class TaskInstanceSchema(ApiObjectSchema):
    execution_date = fields.DateTime()
    dag_id = fields.String()
    state = fields.String(allow_none=True)
    try_number = fields.Integer()
    task_id = fields.String()
    start_date = fields.DateTime(allow_none=True)
    end_date = fields.DateTime(allow_none=True)
    log_body = fields.String(allow_none=True)
    xcom_dict = fields.Dict()


class MetricsSchema(ApiObjectSchema):
    performance = fields.Dict()
    sizes = fields.Dict()


class AirflowExportMetaSchema(ApiObjectSchema):
    airflow_version = fields.String()
    plugin_version = fields.String()
    request_args = fields.Dict()
    metrics = fields.Nested(MetricsSchema)


class InitDagRunsRequestSchema(ApiObjectSchema):
    dags = fields.Nested(DagSchema, many=True)
    dag_runs = fields.Nested(DagRunSchema, many=True)
    task_instances = fields.Nested(TaskInstanceSchema, many=True)

    airflow_export_meta = fields.Nested(AirflowExportMetaSchema, required=False)
    error_message = fields.String(required=False, allow_none=True)
    syncer_type = fields.String(allow_none=True)


class OkResponseSchema(ApiObjectSchema):
    ok = fields.Boolean()


class UpdateDagRunsRequestSchema(ApiObjectSchema):
    dag_runs = fields.Nested(DagRunSchema, many=True)
    task_instances = fields.Nested(TaskInstanceSchema, many=True)
    last_seen_log_id = fields.Integer(allow_none=True)

    airflow_export_meta = fields.Nested(AirflowExportMetaSchema, required=False)
    error_message = fields.String(required=False, allow_none=True)
    syncer_type = fields.String(allow_none=True)
