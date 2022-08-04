# © Copyright Databand.ai, an IBM Company 2022

import hashlib
import logging
import os

from dbnd._core.errors import friendly_error
from dbnd._core.parameter.parameter_builder import output
from dbnd._core.task.task import Task
from dbnd._core.utils.task_utils import to_targets
from dbnd._core.utils.traversing import flatten
from targets.multi_target import MultiTarget


logger = logging.getLogger(__name__)


class DataSourceTask(Task):
    """Subclass for references to data sources."""

    _conf__task_type_name = "data"
    task_target_date = None


class _DataSource(DataSourceTask):
    data = output

    def run(self):

        missing = []
        for partial_output in flatten(self.data):
            if not partial_output.exists():
                missing.append(partial_output)
        if missing:
            raise friendly_error.task_data_source_not_exists(
                self, missing, downstream=self.task_dag.downstream
            )


def data_source(data):
    a, b = os.path.split(str(data))
    a = hashlib.md5(a.encode("utf-8")).hexdigest()[:6]  # nosec B324
    logger.debug("Creating external task %s", data)
    return _DataSource(data=data, task_name="file_%s__%s" % (a, b))


class DataTask(Task):
    pass


def data_combine(inputs, sort=False):
    targets = flatten(to_targets(inputs))
    if sort:
        targets = sorted(targets, key=lambda x: x.path)
    data = MultiTarget(targets)
    return data
