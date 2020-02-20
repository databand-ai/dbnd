import logging
import typing

from typing import Any

from dbnd._core.constants import DbndTargetOperationStatus, DbndTargetOperationType
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.tracking.metrics import Metric
from dbnd._core.tracking.tracking_store import TrackingStore
from dbnd._core.utils.timezone import utcnow


if typing.TYPE_CHECKING:
    from targets import Target
    from dbnd._core.parameter.parameter_definition import ParameterDefinition

logger = logging.getLogger(__name__)


class TaskRunTracker(TaskRunCtrl):
    def __init__(self, task_run, tracking_store):
        super(TaskRunTracker, self).__init__(task_run=task_run)
        self.tracking_store = tracking_store  # type: TrackingStore

    def task_run_url(self):
        run_tracker = self.run.tracker
        if not run_tracker.databand_url:
            return None

        return "{databand_url}/app/jobs/{root_task_name}/{run_uid}/{task_run_uid}".format(
            databand_url=run_tracker.databand_url,
            root_task_name=self.run.job_name,
            run_uid=self.run.run_uid,
            task_run_uid=self.task_run_uid,
        )

    # Task Handlers
    def save_task_run_log(self, log_preview):
        self.tracking_store.save_task_run_log(
            task_run=self.task_run, log_body=log_preview
        )

    def log_target(self, parameter, target, value, operation_type, operation_status):
        # type: (TaskRunTracker, ParameterDefinition, Target, Any, DbndTargetOperationType, DbndTargetOperationStatus) -> None
        try:
            target_meta = parameter.get_value_meta(value)
            target.target_meta = target_meta
            self.tracking_store.log_target(
                task_run=self.task_run,
                target=target,
                target_meta=target_meta,
                operation_type=operation_type,
                operation_status=operation_status,
                param_name=parameter.name,
                task_def_uid=parameter.task_definition_uid,
            )
        except Exception as ex:
            logger.warning(
                "Error occurred during target metrics save for %s: %s" % (target, ex)
            )

    def _log_metric(self, key, value, timestamp=None, source=None):
        metric = Metric(key=key, timestamp=timestamp or utcnow(), value=value)

        self.tracking_store.log_metric(
            task_run=self.task_run, metric=metric, source=source
        )

    def log_artifact(self, name, artifact):
        # file storage will save file
        # db will save path
        artifact_target = self.task_run.meta_files.get_artifact_target(name)
        self.tracking_store.log_artifact(
            task_run=self.task_run,
            name=name,
            artifact=artifact,
            artifact_target=artifact_target,
        )

    def log_metric(self, key, value, timestamp=None, source=None):
        logger.info(
            "{}{}Metric '{}'='{}'".format(
                (source or "").capitalize(), " " * bool(source), key, value
            )
        )
        self._log_metric(key, value, timestamp=timestamp, source=source)

    def log_dataframe(self, key, df):
        logger.info("Dataframe '{}'='{}".format(key, df.shape))
        if len(df.shape) == 1:
            self._log_metric(key, df.shape[0][1])
            return

        self.log_metric(key, df.shape)
        for dim, size in enumerate(df.shape):
            self._log_metric("%s[%s]" % (key, dim), size)
