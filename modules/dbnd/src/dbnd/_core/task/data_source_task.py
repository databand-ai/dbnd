import hashlib
import logging
import os

from dbnd import output
from dbnd._core.errors import friendly_error
from dbnd._core.task.task import Task
from dbnd._core.utils.traversing import flatten


logger = logging.getLogger(__name__)


class DataSourceTask(Task):
    """
    Subclass for references to data sources.
    """

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
    a = hashlib.md5(a.encode("utf-8")).hexdigest()[:6]
    logger.debug("Creating external task %s", data)
    return _DataSource(data=data, task_name="file_%s__%s" % (a, b))
