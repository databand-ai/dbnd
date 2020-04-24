# ORIGIN: https://github.com/databricks/mlflow : mlflow/store/tracking_store_file.py
from __future__ import print_function

import logging
import os
import re
import time

from datetime import datetime

import six
import yaml

from six import BytesIO

from dbnd._core.constants import TaskRunState
from dbnd._core.errors import DatabandError, DatabandRuntimeError
from dbnd._core.task_run.task_run_meta_files import TaskRunMetaFiles
from dbnd._core.tracking.metrics import Artifact, Metric
from dbnd._core.tracking.tracking_info_convertor import (
    build_task_run_info,
    task_to_task_def,
)
from dbnd._core.tracking.tracking_store import TrackingStore
from dbnd.api.serialization.task import TaskDefinitionInfoSchema, TaskRunInfoSchema
from targets import target


logger = logging.getLogger(__name__)
try:
    from matplotlib.figure import Figure

    PYPLOT_INSTALLED = True
except ImportError:
    PYPLOT_INSTALLED = False


def _parse_metric(value):
    try:
        return float(value)
    except ValueError:
        return value


_METRICS_RE = re.compile(r"(\d+)\s+(.+)")


class FileTrackingStore(TrackingStore):
    def set_task_run_state(self, task_run, state, error=None, timestamp=None):
        if state == TaskRunState.RUNNING:
            self.dump_task_run_info(task_run)

    def dump_task_run_info(self, task_run):

        from dbnd.api.serialization.run import RunInfoSchema

        info = {
            "task_run": TaskRunInfoSchema().dump(build_task_run_info(task_run)).data,
            "task_def": (
                TaskDefinitionInfoSchema()
                .dump(task_to_task_def(task_run.run.context, task_run.task))
                .data
            ),
            "run": RunInfoSchema().dump(task_run.run).data,
        }
        # old implementation, replace with "dbnd_tracking" request

        meta_data_file = task_run.meta_files.get_meta_data_file()
        with meta_data_file.open("w") as yaml_file:
            yaml.dump(info, yaml_file, default_flow_style=False)

    def log_metric(self, task_run, metric, source=None):
        metric_path = task_run.meta_files.get_metric_target(metric.key, source=source)
        timestamp = int(time.mktime(metric.timestamp.timetuple()))
        value = "%s %s\n" % (timestamp, metric.value)

        data = value
        if metric_path.exists():
            data = metric_path.read() + value
        metric_path.write(data)

    def log_artifact(self, task_run, name, artifact, artifact_target):
        artifact_target.mkdir_parent()

        if isinstance(artifact, six.string_types):
            from targets.dir_target import DirTarget

            artifact_target_source = target(artifact)
            if isinstance(artifact_target_source, DirTarget):
                artifact_target_source.copy(artifact_target)
            else:
                data = artifact_target_source.read()
                artifact_target.write(data)

            return artifact_target

        if PYPLOT_INSTALLED and isinstance(artifact, Figure):
            temp = BytesIO()
            artifact.savefig(temp)
            temp.seek(0)
            artifact_target.write(temp.read(), mode="wb")
            return artifact_target

        raise DatabandRuntimeError(
            "Could not recognize artifact of type %s, must be string or matplotlib Figure"
            % type(artifact)
        )

    def is_ready(self):
        return True


class TaskRunMetricsFileStoreReader(object):
    def __init__(self, attempt_folder, **kwargs):
        super(TaskRunMetricsFileStoreReader, self).__init__(**kwargs)
        self.meta = TaskRunMetaFiles(attempt_folder)

    def _get_all_metrics_names(self, source=None):
        metrics_root = self.meta.get_metric_folder(source=source)
        all_files = [os.path.basename(str(p)) for p in metrics_root.list_partitions()]
        return all_files

    def get_metric_history(self, key, source=None):
        metric_target = self.meta.get_metric_target(key, source=source)
        if not metric_target.exists():
            raise DatabandError("Metric '%s' not found" % key)
        metric_data = metric_target.readlines()
        rsl = []
        for pair in metric_data:
            ts, val = pair.strip().split(" ")
            rsl.append(Metric(key, float(val), datetime.fromtimestamp(int(ts))))
        return rsl

    def get_all_metrics_values(self, source=None):
        metrics = []
        for key in self._get_all_metrics_names(source=source):
            try:
                metrics.append(self.get_metric(key, source=source))
            except Exception as ex:
                raise DatabandError(
                    "Failed to read metrics for %s at %s" % (key, self.meta.root),
                    nested_exceptions=ex,
                )
        return {m.key: m.value for m in metrics}

    def get_run_info(self):

        from dbnd.api.serialization.run import RunInfoSchema

        with self.meta.get_meta_data_file().open("r") as yaml_file:
            return RunInfoSchema().load(**yaml.load(yaml_file))

    def get_metric(self, key, source=None):
        metric_target = self.meta.get_metric_target(key, source=source)
        if not metric_target.exists():
            raise DatabandRuntimeError("Metric '%s' not found" % key)
        metric_data = metric_target.readlines()
        if len(metric_data) == 0:
            raise DatabandRuntimeError("Metric '%s' is malformed. No data found." % key)
        first_line = metric_data[0]

        metric_parsed = _METRICS_RE.match(first_line)
        if not metric_parsed:
            raise DatabandRuntimeError(
                "Metric '%s' is malformed. Expected format: 'TS VALUE', got='%s'"
                % (key, first_line)
            )

        timestamp, val = metric_parsed.groups()

        return Metric(
            key=key,
            value=_parse_metric(val),
            timestamp=datetime.fromtimestamp(int(timestamp)),
        )

    def get_artifact(self, name):
        artifact_target = self.meta.get_artifact_target(name)
        if not artifact_target.exists():
            raise DatabandError("Artifact '%s' not found" % name)
        return Artifact(artifact_target.path)


def read_task_metrics(attempt_folder, source=None):
    return TaskRunMetricsFileStoreReader(
        attempt_folder=target(attempt_folder)
    ).get_all_metrics_values(source=source)
