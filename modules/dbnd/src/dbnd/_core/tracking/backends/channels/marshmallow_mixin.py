from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel
from dbnd.api.tracking_api import (
    add_task_runs_schema,
    airflow_task_infos_schema,
    heartbeat_schema,
    init_run_schema,
    log_artifact_schema,
    log_datasets_schema,
    log_metrics_schema,
    log_targets_schema,
    save_external_links_schema,
    save_task_run_log_schema,
    scheduled_job_args_schema,
    set_run_state_schema,
    set_task_run_reused_schema,
    set_unfinished_tasks_state_schema,
    update_task_run_attempts_schema,
)


class MarshmallowMixin:
    SCHEMA_BY_HANDLER_NAME = {
        TrackingChannel.init_scheduled_job.__name__: scheduled_job_args_schema,
        TrackingChannel.init_run.__name__: init_run_schema,
        TrackingChannel.add_task_runs.__name__: add_task_runs_schema,
        TrackingChannel.set_run_state.__name__: set_run_state_schema,
        TrackingChannel.set_task_reused.__name__: set_task_run_reused_schema,
        TrackingChannel.update_task_run_attempts.__name__: update_task_run_attempts_schema,
        TrackingChannel.set_unfinished_tasks_state.__name__: set_unfinished_tasks_state_schema,
        TrackingChannel.save_task_run_log.__name__: save_task_run_log_schema,
        TrackingChannel.save_external_links.__name__: save_external_links_schema,
        TrackingChannel.log_datasets.__name__: log_datasets_schema,
        TrackingChannel.log_targets.__name__: log_targets_schema,
        TrackingChannel.log_metrics.__name__: log_metrics_schema,
        TrackingChannel.log_artifact.__name__: log_artifact_schema,
        TrackingChannel.heartbeat.__name__: heartbeat_schema,
        TrackingChannel.save_airflow_task_infos.__name__: airflow_task_infos_schema,
    }

    def get_schema_by_handler_name(self, handler_name):
        return self.SCHEMA_BY_HANDLER_NAME[handler_name]
