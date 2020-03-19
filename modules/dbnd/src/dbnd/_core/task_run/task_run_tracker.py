import logging
import typing

from typing import Any

from dbnd._core.constants import DbndTargetOperationStatus, DbndTargetOperationType
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.tracking.metrics import Metric
from dbnd._core.tracking.tracking_store import TrackingStore
from dbnd._core.utils.timezone import utcnow
from targets.target_meta import TargetMeta
from targets.values import get_value_type_of_obj


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
        self._log_metric(key, value, timestamp=timestamp, source=source)

    def log_dataframe(self, key, df, with_preview=True):
        value_meta = get_value_meta_for_metric(key, df, with_preview=with_preview)
        if not value_meta:
            return

        if value_meta.data_dimensions:
            self._log_metric("%s.shape" % key, value_meta.data_dimensions)
            for dim, size in enumerate(value_meta.data_dimensions):
                self._log_metric("%s.shape[%s]" % (key, dim), size)

        self._log_metric("%s.schema" % key, value_meta.data_schema)
        if with_preview:
            self._log_metric("%s.preview" % key, value_meta.value_preview)


def get_value_meta_for_metric(key, value, with_preview=True, with_data_hash=False):
    value_type = get_value_type_of_obj(value)
    if value_type is None:
        logger.info(
            "Can't detect known type for '%s' with type='%s' ", key, type(value)
        )
        return None
    try:
        data_dimensions = value_type.get_data_dimensions(value)
        if data_dimensions is not None:
            data_dimensions = list(data_dimensions)

        try:
            preview = value_type.to_preview(value) if with_preview else None
            data_hash = value_type.get_data_hash(value) if with_data_hash else None
        except Exception as ex:
            logger.info(
                "Failed to calculate data preview for '%s' with type='%s'"
                " ( detected as %s): %s",
                key,
                type(value),
                value_type,
                ex,
            )
            data_hash = preview = None

        data_schema = value_type.get_data_schema(value)

        return TargetMeta(
            value_preview=preview,
            data_dimensions=data_dimensions,
            data_schema=data_schema,
            data_hash=data_hash,
        )

    except Exception as ex:
        logger.info(
            "Failed to build value meta info for '%s' with type='%s'"
            " ( detected as %s): %s",
            key,
            type(value),
            value_type,
            ex,
        )
    return None
