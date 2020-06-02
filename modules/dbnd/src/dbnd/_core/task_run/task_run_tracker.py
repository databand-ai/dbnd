import logging
import typing

from dbnd._core.constants import (
    DbndTargetOperationStatus,
    DbndTargetOperationType,
    MetricSource,
)
from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.tracking.metrics import Metric
from dbnd._core.tracking.tracking_store import TrackingStore
from dbnd._core.utils.timezone import utcnow
from targets import Target
from targets.values import get_value_type_of_obj


if typing.TYPE_CHECKING:
    from datetime import datetime
    from typing import Any, Optional, Union
    import pandas as pd
    import pyspark.sql as spark

    from targets.value_meta import ValueMetaConf

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
        features_conf = self.settings.features
        if not features_conf.log_value_meta:
            return
        try:
            value_type = parameter.value_type
            meta_conf = features_conf.get_value_meta_conf(
                parameter.value_meta_conf, value_type=value_type, target=target
            )

            target_meta = value_type.get_value_meta(value, meta_conf=meta_conf)
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
            log_exception(
                "Error occurred during target logging for %s" % (target,),
                ex,
                non_critical=True,
            )

    def _log_metric(self, key, value, timestamp=None, source=None, is_json=False):
        # type: (str, Any, Optional[datetime], Optional[MetricSource], Optional[bool]) -> None
        if is_json:
            metric = Metric(key=key, timestamp=timestamp or utcnow(), value_json=value)
        else:
            metric = Metric(key=key, timestamp=timestamp or utcnow(), value=value)

        self.tracking_store.log_metric(
            task_run=self.task_run, metric=metric, source=source
        )

    def log_artifact(self, name, artifact):
        try:
            # file storage will save file
            # db will save path
            artifact_target = self.task_run.meta_files.get_artifact_target(name)
            self.tracking_store.log_artifact(
                task_run=self.task_run,
                name=name,
                artifact=artifact,
                artifact_target=artifact_target,
            )
        except Exception as ex:
            log_exception(
                "Error occurred during log_artifact for %s" % (name,),
                ex,
                non_critical=True,
            )

    def log_metric(self, key, value, timestamp=None, source=None):
        try:
            self._log_metric(key, value, timestamp=timestamp, source=source)
        except Exception as ex:
            log_exception(
                "Error occurred during log_metric for %s" % (key,),
                ex,
                non_critical=True,
            )

    def log_dataframe(self, key, df, meta_conf):
        # type: (str, Union[pd.DataFrame, spark.DataFrame], ValueMetaConf) -> None
        try:
            # Combine meta_conf with the config settings
            meta_conf = self.settings.features.get_value_meta_conf(meta_conf)
            value_meta = get_value_meta_for_metric(key, df, meta_conf=meta_conf)
            if not value_meta:
                return

            # TODO: Performance optimization opportunity: batch log_metric API calls
            if value_meta.data_dimensions:
                self._log_metric("%s.shape" % key, value_meta.data_dimensions)
                for dim, size in enumerate(value_meta.data_dimensions):
                    self._log_metric("%s.shape[%s]" % (key, dim), size)
            if meta_conf.log_schema:
                self._log_metric(
                    "%s.schema" % key, value_meta.data_schema, is_json=True
                )
            if meta_conf.log_preview:
                self._log_metric("%s.preview" % key, value_meta.value_preview)
            if meta_conf.log_df_hist:
                self._log_histograms(key, value_meta)
        except Exception as ex:
            log_exception(
                "Error occurred during log_dataframe for %s" % (key,),
                ex,
                non_critical=True,
            )

    def _log_histograms(self, df_name, value_meta):
        self._log_metric(
            "%s.stats" % df_name,
            value_meta.descriptive_stats,
            source=MetricSource.histograms,
            is_json=True,
        )
        self._log_metric(
            "%s.histograms" % df_name,
            value_meta.histograms,
            source=MetricSource.histograms,
            is_json=True,
        )
        for column, stats in value_meta.descriptive_stats.items():
            for stat, value in stats.items():
                self._log_metric(
                    "{}.{}.{}".format(df_name, column, stat),
                    value,
                    source=MetricSource.histograms,
                )


def get_value_meta_for_metric(key, value, meta_conf):
    value_type = get_value_type_of_obj(value)
    if value_type is None:
        logger.info(
            "Can't detect known type for '%s' with type='%s' ", key, type(value)
        )
        return None
    try:
        return value_type.get_value_meta(value, meta_conf=meta_conf)

    except Exception as ex:
        logger.info(
            "Failed to get value meta info for '%s' with type='%s'"
            " ( detected as %s): %s",
            key,
            type(value),
            value_type,
            ex,
        )
    return None
