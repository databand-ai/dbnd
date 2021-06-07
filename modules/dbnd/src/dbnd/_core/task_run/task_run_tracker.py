import logging
import typing

import six

from dbnd._core.constants import (
    DbndDatasetOperationType,
    DbndTargetOperationStatus,
    DbndTargetOperationType,
    MetricSource,
)
from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.settings.tracking_config import get_value_meta
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.tracking.schemas.metrics import Metric
from dbnd._core.utils.timezone import utcnow
from targets import Target
from targets.value_meta import ValueMeta, ValueMetaConf


if typing.TYPE_CHECKING:
    from dbnd._core.tracking.backends import TrackingStore
    from dbnd_postgres.postgres_values import PostgresTable
    from dbnd_snowflake.snowflake_values import SnowflakeTable

    from datetime import datetime
    from typing import Any, Optional, Union, List, Dict
    import pandas as pd
    import pyspark.sql as spark

logger = logging.getLogger(__name__)


class TaskRunTracker(TaskRunCtrl):
    def __init__(self, task_run, tracking_store):
        super(TaskRunTracker, self).__init__(task_run=task_run)
        self.tracking_store = tracking_store  # type: TrackingStore

    def task_run_url(self):
        run_tracker = self.run.tracker
        if not run_tracker.databand_url:
            return None

        return "{databand_url}/app/jobs/{job_name}/{run_uid}/{task_run_uid}".format(
            databand_url=run_tracker.databand_url,
            job_name=self.run.job_name,
            run_uid=self.run.run_uid,
            task_run_uid=self.task_run_uid,
        )

    # Task Handlers
    def save_task_run_log(self, log_preview, local_log_path=None):
        self.tracking_store.save_task_run_log(
            task_run=self.task_run, log_body=log_preview, local_log_path=local_log_path
        )

    def log_parameter_data(
        self, parameter, target, value, operation_type, operation_status
    ):
        # type: (TaskRunTracker, ParameterDefinition, Target, Any, DbndTargetOperationType, DbndTargetOperationStatus) -> None
        tracking_conf = self.settings.tracking
        if not tracking_conf.log_value_meta or value is None:
            return

        try:
            target.target_meta = get_value_meta(
                value,
                parameter.value_meta_conf,
                tracking_config=tracking_conf,
                value_type=parameter.value_type,
                target=target,
            )
            # FIXME If we failed to get target meta for some reason, target operation won't be logged!
            if target.target_meta is None:
                return

            self.tracking_store.log_target(
                task_run=self.task_run,
                target=target,
                target_meta=target.target_meta,
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

    def _log_metrics(self, metrics):
        # type: (List[Metric]) -> None
        return self.tracking_store.log_metrics(task_run=self.task_run, metrics=metrics)

    def log_metrics(self, metrics_dict, source=None, timestamp=None):
        # type: (Dict[str, Any], Optional[str], Optional[datetime]) -> None
        """
        Logs all the metrics in the metrics dict to the tracker.
        @param metrics_dict: name-value pairs of metrics to log
        @param source: optional name of the metrics source
        @param timestamp: optional timestamp of the metrics
        """
        metrics = [
            Metric(key=key, value=value, source=source, timestamp=timestamp or utcnow())
            for key, value in six.iteritems(metrics_dict)
        ]

        return self.tracking_store.log_metrics(task_run=self.task_run, metrics=metrics)

    def log_metrics(self, metrics_dict, source=None, timestamp=None):
        # type: (Dict[str, Any], Optional[str], Optional[datetime]) -> None
        """
        Logs all the metrics in the metrics dict to the tracker.
        @param metrics_dict: name-value pairs of metrics to log
        @param source: optional name of the metrics source
        @param timestamp: optional timestamp of the metrics
        """
        metrics = [
            Metric(key=key, value=value, source=source, timestamp=timestamp or utcnow())
            for key, value in six.iteritems(metrics_dict)
        ]

        return self.tracking_store.log_metrics(task_run=self.task_run, metrics=metrics)

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
        # type: (str, Any, Optional[datetime], Optional[MetricSource]) -> None
        try:
            self.log_metrics({key: value}, source, timestamp)
        except Exception as ex:
            log_exception(
                "Error occurred during log_metric for %s" % (key,),
                ex,
                non_critical=True,
            )

    def log_data(
        self,
        key,  # type: str
        data,  # type: Union[pd.DataFrame, spark.DataFrame, PostgresTable, SnowflakeTable]
        meta_conf,  # type: ValueMetaConf
        path=None,  # type: Optional[Union[Target,str]]
        operation_type=DbndTargetOperationType.read,  # type: DbndTargetOperationType
        operation_status=DbndTargetOperationStatus.OK,  # type: DbndTargetOperationStatus
        raise_on_error=False,  # type: bool
    ):  # type: (...) -> None
        try:
            # Combine meta_conf with the config settings
            value_meta = get_value_meta(
                data, meta_conf, tracking_config=self.settings.tracking
            )
            if not value_meta:
                logger.warning(
                    "Couldn't log the wanted data {name}, reason - can't log objects of type {value_type} ".format(
                        name=key, value_type=type(data)
                    )
                )
                return

            ts = utcnow()

            if path:
                self.tracking_store.log_target(
                    task_run=self.task_run,
                    target=path,
                    target_meta=value_meta,
                    operation_type=operation_type,
                    operation_status=operation_status,
                    param_name=key,
                )
            metrics = value_meta.build_metrics_for_key(key, meta_conf)

            if metrics["user"]:
                self._log_metrics(metrics["user"])

            if metrics["histograms"]:
                self.tracking_store.log_histograms(
                    task_run=self.task_run,
                    key=key,
                    value_meta=value_meta,
                    timestamp=ts,
                )

            if not (metrics["user"] or metrics["histograms"] or path):
                logger.info("No metrics to log_data(key={}, data={})".format(key, data))

        except Exception as ex:
            log_exception(
                "Error occurred during log_dataframe for %s" % (key,),
                ex,
                non_critical=not raise_on_error,
            )
            if raise_on_error:
                raise

    def log_dataset(
        self,
        operation_path,  # type: Union[Target,str]
        operation_type,  # type: DbndDatasetOperationType
        operation_status,  # type: DbndTargetOperationStatus
        data=None,
        meta_conf=None,
    ):
        data_meta = None
        if data is not None and meta_conf is not None:
            # Combine meta_conf with the config settings
            try:
                data_meta = get_value_meta(
                    data, meta_conf, tracking_config=self.settings.tracking
                )
            except Exception:
                logger.exception(
                    "Failed to get value meta info for operation_path {}".format(
                        str(operation_path)
                    )
                )

        if data_meta is None:
            data_meta = ValueMeta("")

        self.tracking_store.log_dataset(
            task_run=self.task_run,
            operation_path=operation_path,
            data_meta=data_meta,
            operation_type=operation_type,
            operation_status=operation_status,
        )
