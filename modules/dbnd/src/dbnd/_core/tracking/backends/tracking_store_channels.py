import datetime
import logging
import typing

from dbnd._core.constants import DbndTargetOperationStatus, DbndTargetOperationType
from dbnd._core.tracking.backends.abstract_tracking_store import TrackingStore
from dbnd._core.tracking.backends.channels.abstract_channel import TrackingChannel
from dbnd._core.tracking.schemas.metrics import Metric
from dbnd._core.tracking.tracking_info_convertor import TrackingInfoBuilder
from dbnd._core.utils import json_utils
from dbnd._core.utils.timezone import utcnow
from dbnd.api.tracking_api import LogTargetArgs, TaskRunAttemptUpdateArgs
from targets.value_meta import ValueMeta


if typing.TYPE_CHECKING:
    from typing import List, Optional, Iterable
    from uuid import UUID

    from dbnd._core.constants import TaskRunState
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd._core.task_run.task_run_error import TaskRunError

logger = logging.getLogger(__name__)


class TrackingStoreThroughChannel(TrackingStore):
    """Track data to Tracking API"""

    def __init__(self, channel):
        # type: (TrackingChannel) -> None
        self.channel = channel

    def init_scheduled_job(self, scheduled_job, update_existing):
        return self._m(
            self.channel.init_scheduled_job,
            scheduled_job_args=scheduled_job,
            update_existing=update_existing,
        )

    def init_run(self, run):
        init_args = TrackingInfoBuilder(run).build_init_args()
        return self.init_run_from_args(init_args=init_args)

    def init_run_from_args(self, init_args):
        return self._m(self.channel.init_run, init_args=init_args)

    def add_task_runs(self, run, task_runs):
        task_runs_info = TrackingInfoBuilder(run).build_task_runs_info(
            task_runs=task_runs, dynamic_task_run_update=True
        )
        return self._m(
            self.channel.add_task_runs,
            task_runs_info=task_runs_info,
            source=run.source,
        )

    def set_run_state(self, run, state, error=None, timestamp=None):
        return self._m(
            self.channel.set_run_state,
            run_uid=run.run_uid,
            state=state,
            timestamp=timestamp,
        )

    def set_task_reused(self, task_run):
        return self._m(
            self.channel.set_task_reused,
            task_run_uid=task_run.task_run_uid,
            task_outputs_signature=task_run.task.task_meta.task_outputs_signature,
        )

    def set_task_run_state(self, task_run, state, error=None, timestamp=None):
        # type: (TaskRun, TaskRunState, TaskRunError, datetime.datetime) -> None
        return self._m(
            self.channel.update_task_run_attempts,
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

    def set_unfinished_tasks_state(self, run_uid, state):
        return self._m(
            self.channel.set_unfinished_tasks_state,
            run_uid=run_uid,
            state=state,
            timestamp=utcnow(),
        )

    def update_task_run_attempts(self, task_run_attempt_updates):
        return self._m(
            self.channel.update_task_run_attempts,
            task_run_attempt_updates=task_run_attempt_updates,
        )

    def save_task_run_log(self, task_run, log_body):
        return self._m(
            self.channel.save_task_run_log,
            task_run_attempt_uid=task_run.task_run_attempt_uid,
            log_body=log_body,
        )

    def save_external_links(self, task_run, external_links_dict):
        return self._m(
            self.channel.save_external_links,
            task_run_attempt_uid=task_run.task_run_attempt_uid,
            external_links_dict=external_links_dict,
        )

    def log_target(
        self,
        task_run,
        target,
        target_meta,  # type: ValueMeta
        operation_type,  # type: DbndTargetOperationType
        operation_status,  # type: DbndTargetOperationStatus
        param_name=None,  # type: Optional[str]
        task_def_uid=None,  # type: Optional[UUID]
    ):
        data_schema = (
            json_utils.dumps(target_meta.data_schema)
            if target_meta.data_schema is not None
            else None
        )
        target_info = LogTargetArgs(
            run_uid=task_run.run.run_uid,
            task_run_uid=task_run.task_run_uid,
            task_run_name=task_run.job_name,
            task_run_attempt_uid=task_run.task_run_attempt_uid,
            task_def_uid=task_def_uid,
            param_name=param_name,
            target_path=str(target),
            operation_type=operation_type,
            operation_status=operation_status,
            value_preview=target_meta.value_preview,
            data_dimensions=target_meta.data_dimensions,
            data_schema=data_schema,
            data_hash=target_meta.data_hash,
        )
        res = self.log_targets(targets_info=[target_info])
        if target_meta.histograms:
            self.log_histograms(task_run, param_name, target_meta, utcnow())
        return res

    def log_targets(self, targets_info):  # type: (List[LogTargetArgs]) -> None
        return self._m(self.channel.log_targets, targets_info=targets_info)

    def log_histograms(self, task_run, key, value_meta, timestamp):
        value_meta_metrics = value_meta.build_metrics_for_key(key)
        if value_meta_metrics["histograms"]:
            self.log_metrics(
                task_run=task_run, metrics=value_meta_metrics["histograms"]
            )

    def log_metrics(self, task_run, metrics):
        # type: (TaskRun, Iterable[Metric]) -> None
        metrics_info = [
            {
                "task_run_attempt_uid": task_run.task_run_attempt_uid,
                "metric": metric,
                "source": metric.source,
            }
            for metric in metrics
        ]
        return self._m(self.channel.log_metrics, metrics_info=metrics_info,)

    def log_artifact(self, task_run, name, artifact, artifact_target):
        return self._m(
            self.channel.log_artifact,
            task_run_attempt_uid=task_run.task_run_attempt_uid,
            name=name,
            path=artifact_target.path,
        )

    def heartbeat(self, run_uid):
        return self._m(self.channel.heartbeat, run_uid=run_uid)

    def save_airflow_task_infos(self, airflow_task_infos, source, base_url):
        return self._m(
            self.channel.save_airflow_task_infos,
            airflow_task_infos=airflow_task_infos,
            source=source,
            base_url=base_url,
        )

    def save_airflow_monitor_data(
        self, airflow_monitor_data, airflow_base_url, last_sync_time
    ):
        return self._m(
            self.channel.save_airflow_monitor_data,
            airflow_export_data=airflow_monitor_data,
            airflow_base_url=airflow_base_url,
            last_sync_time=last_sync_time,
        )

    def _m(self, channel_call, **req_kwargs):
        """
        Marshall and call channel function
        :return:
        """
        req_schema = self.channel.get_schema_by_handler_name(channel_call.__name__)
        marsh = req_schema.dump(req_kwargs)
        resp = channel_call(marsh.data)
        # if resp_schema and resp:
        #     resp = resp_schema.load(resp)
        return resp

    def is_ready(self):
        return self.channel.is_ready()

    def __str__(self):
        return "TrackingStoreThroughChannel with channel=%s" % (str(self.channel),)

    @staticmethod
    def build_with_disabled_channel():
        from dbnd._core.tracking.backends.channels.tracking_disabled_channel import (
            DisabledTrackingChannel,
        )

        return TrackingStoreThroughChannel(channel=DisabledTrackingChannel())

    @staticmethod
    def build_with_console_debug_channel():
        from dbnd._core.tracking.backends.channels.tracking_debug_channel import (
            ConsoleDebugTrackingChannel,
        )

        return TrackingStoreThroughChannel(channel=ConsoleDebugTrackingChannel())

    @staticmethod
    def build_with_web_channel():
        from dbnd._core.tracking.backends.channels.tracking_web_channel import (
            TrackingWebChannel,
        )

        return TrackingStoreThroughChannel(channel=TrackingWebChannel())

    @staticmethod
    def build_with_proto_web_channel():
        from dbnd._core.tracking.backends.channels.tracking_proto_web_channel import (
            TrackingProtoWebChannel,
        )

        return TrackingStoreThroughChannel(channel=TrackingProtoWebChannel())
