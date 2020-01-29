import datetime
import logging
import typing

from dbnd._core.tracking.tracking_store import TrackingStore
from dbnd._core.utils.timezone import utcnow
from dbnd.api.tracking_api import (
    TaskRunAttemptUpdateArgs,
    add_task_runs_schema,
    heartbeat_schema,
    init_run_schema,
    log_artifact_schema,
    log_metric_schema,
    scheduled_job_args_schema,
    set_run_state_schema,
    update_task_run_attempts_schema,
)


if typing.TYPE_CHECKING:
    from dbnd._core.constants import TaskRunState
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd._core.task_run.task_run_error import TaskRunError

logger = logging.getLogger(__name__)


class TrackingStoreApi(TrackingStore):
    """Track data to Tracking API"""

    def __init__(self, channel):
        self.channel = channel

    def init_scheduled_job(self, scheduled_job):
        return self._m(
            self.channel.init_scheduled_job,
            scheduled_job_args_schema,
            scheduled_job_args=scheduled_job,
        )

    def init_run(self, run):
        from dbnd._core.tracking.tracking_info_convertor import TrackingInfoBuilder

        init_args = TrackingInfoBuilder(run).build_init_args()

        return self.init_run_from_args(init_args=init_args)

    def init_run_from_args(self, init_args):
        return self._m(self.channel.init_run, init_run_schema, init_args=init_args)

    def add_task_runs(self, run, task_runs):
        from dbnd._core.tracking.tracking_info_convertor import TrackingInfoBuilder

        task_runs_info = TrackingInfoBuilder(run).build_task_runs_info(
            task_runs=task_runs, dynamic_task_run_update=True
        )

        return self._m(
            self.channel.add_task_runs,
            add_task_runs_schema,
            task_runs_info=task_runs_info,
        )

    def set_run_state(self, run, state, error=None, timestamp=None):
        return self._m(
            self.channel.set_run_state,
            set_run_state_schema,
            run_uid=run.run_uid,
            state=state,
            timestamp=timestamp,
        )

    def set_task_reused(self, task_run):
        from dbnd.api.tracking_api import set_task_run_reused_schema

        return self._m(
            self.channel.set_task_reused,
            set_task_run_reused_schema,
            task_run_uid=task_run.task_run_uid,
            task_outputs_signature=task_run.task.task_meta.task_outputs_signature,
        )

    def set_task_run_state(self, task_run, state, error=None, timestamp=None):
        # type: (TaskRun, TaskRunState, TaskRunError, datetime.datetime) -> None
        return self._m(
            self.channel.update_task_run_attempts,
            update_task_run_attempts_schema,
            task_run_attempt_updates=[
                TaskRunAttemptUpdateArgs(
                    task_run_uid=task_run.task_run_uid,
                    task_run_attempt_uid=task_run.task_run_attempt_uid,
                    state=state,
                    error=error.as_error_info() if error else None,
                    timestamp=timestamp or utcnow(),
                )
            ],
        )

    def set_task_run_states(self, task_runs):
        return self._m(
            self.channel.update_task_run_attempts,
            update_task_run_attempts_schema,
            task_run_attempt_updates=[
                TaskRunAttemptUpdateArgs(
                    task_run_uid=task_run.task_run_uid,
                    task_run_attempt_uid=task_run.task_run_attempt_uid,
                    state=task_run.task_run_state,
                    timestamp=utcnow(),
                )
                for task_run in task_runs
            ],
        )

    def update_task_run_attempts(self, task_run_attempt_updates):
        return self._m(
            self.channel.update_task_run_attempts,
            update_task_run_attempts_schema,
            task_run_attempt_updates=task_run_attempt_updates,
        )

    def save_task_run_log(self, task_run, log_body):
        from dbnd.api.tracking_api import save_task_run_log_schema

        return self._m(
            self.channel.save_task_run_log,
            save_task_run_log_schema,
            task_run_attempt_uid=task_run.task_run_attempt_uid,
            log_body=log_body,
        )

    def save_external_links(self, task_run, external_links_dict):
        from dbnd.api.tracking_api import save_external_links_schema

        return self._m(
            self.channel.save_external_links,
            save_external_links_schema,
            task_run_attempt_uid=task_run.task_run_attempt_uid,
            external_links_dict=external_links_dict,
        )

    def log_target_metrics(self, task_run, target, value_metrics):
        from dbnd.api.tracking_api import log_target_metrics_schema

        return self._m(
            self.channel.log_target_metrics,
            log_target_metrics_schema,
            task_run_uid=task_run.task_run_uid,
            task_run_attempt_uid=task_run.task_run_attempt_uid,
            target_path=str(target),
            value_preview=value_metrics.value_preview,
            data_dimensions=value_metrics.data_dimensions,
            data_schema=value_metrics.data_schema,
        )

    def log_metric(self, task_run, metric):
        return self._m(
            self.channel.log_metric,
            log_metric_schema,
            task_run_attempt_uid=task_run.task_run_attempt_uid,
            metric=metric,
        )

    def log_artifact(self, task_run, name, artifact, artifact_target):
        return self._m(
            self.channel.log_artifact,
            log_artifact_schema,
            task_run_attempt_uid=task_run.task_run_attempt_uid,
            name=name,
            path=artifact_target.path,
        )

    def heartbeat(self, run_uid):
        return self._m(self.channel.heartbeat, heartbeat_schema, run_uid=run_uid)

    def save_airflow_task_infos(self, airflow_task_infos, is_airflow_synced, base_url):
        from dbnd.api.tracking_api import airflow_task_infos_schema

        return self._m(
            self.channel.save_airflow_task_infos,
            airflow_task_infos_schema,
            airflow_task_infos=airflow_task_infos,
            is_airflow_synced=is_airflow_synced,
            base_url=base_url,
        )

    def _m(self, _channel_call, _req_schema, **req_kwargs):
        """
        Marshall and call channel function
        :return:
        """
        marsh = _req_schema.dump(req_kwargs)
        resp = _channel_call(marsh.data)
        # if resp_schema and resp:
        #     resp = resp_schema.load(resp)
        return resp
