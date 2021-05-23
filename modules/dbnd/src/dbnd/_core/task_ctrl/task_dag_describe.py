from __future__ import print_function

import logging

from dbnd._core.constants import DescribeFormat
from dbnd._core.errors import DatabandSystemError
from dbnd._core.settings import DescribeConfig
from dbnd._core.task_ctrl.task_ctrl import TaskSubCtrl
from dbnd._core.utils.basics.helpers import indent
from dbnd._vendor.termcolor import colored
from dbnd.tasks import DataSourceTask


logger = logging.getLogger(__name__)


def tasks_trail(tasks):
    task_ids = [t.task_id for t in tasks]
    return " -> ".join(task_ids)


class DescribeDagCtrl(TaskSubCtrl):
    def __init__(self, task, describe_format=DescribeFormat.long, complete_status=None):
        super(DescribeDagCtrl, self).__init__(task)

        self.describe_format = describe_format

        # dummy implementation of complete cache
        self._complete_status = complete_status or {}

    @property
    def config(self):
        return self.settings.describe

    def tree_view(self, describe_format=None):
        """
        Shows an ascii tree representation of the DAG
        """
        # TODO: change to treelib implementation

        seen = set()

        def get_downstream(task, level=0):
            task_desc = self._describe_task(task, describe_format=describe_format)
            if task in seen:
                return [(level, "%s (*)" % task_desc)]
            result = [(level, task_desc)]

            seen.add(task)
            level += 1
            count = 0
            task_dag = task.ctrl.task_dag
            for t in task_dag.upstream:
                count += 1
                if isinstance(t, DataSourceTask):
                    continue

                if count > 30:
                    result.append((level, "..(%s tasks).." % len(task_dag.upstream)))
                    break
                result.extend(get_downstream(t, level))

            return result

        result = get_downstream(self.task)

        messages = [indent(msg, "\t" * level) for level, msg in result]
        logger.info(
            "Tasks Graph - (*) represent existing node in the graph run "
            "(green is completed, yellow is going to be submitted):\n%s",
            "\n".join(messages),
        )

    def list_view(self):
        logger.info("List View of the DAG:\n")
        for t in self.task_dag.subdag_tasks():
            logger.info("%s\n" % self._describe_task(t))

    def _get_task_complete(self, task):
        if task.task_id not in self._complete_status:
            try:
                complete = task._complete()
            except Exception as ex:
                logger.warning(
                    "Failed to get complete status for %s: %s", task.task_id, ex
                )
                complete = None

            self._complete_status[task.task_id] = complete
        return self._complete_status[task.task_id]

    def _describe_task(self, task, describe_format=None, msg=None, color=None):
        describe_format = describe_format or self.describe_format
        describe_config = self.config  # type: DescribeConfig
        msg = msg or ""

        if color is None:
            color = "white"
            if not describe_config.no_checks:
                color = "green" if self._get_task_complete(task) else "cyan"

        if describe_format == DescribeFormat.short:
            return colored(str(task.task_id), color)

        if (
            describe_format == DescribeFormat.long
            or describe_format == DescribeFormat.verbose
        ):
            title = "%s - %s" % (task.task_name, task.task_id)
            if task.task_name != task.get_task_family():
                title += "(%s)" % task.get_task_family()
            if msg:
                title += ": %s" % msg
            return task.ctrl.visualiser.banner(
                title, color=color, verbose=describe_format == DescribeFormat.verbose
            )

        raise DatabandSystemError("Not supported format mode %s" % self.describe_format)

    def describe_dag(self):
        # print short tree
        self.tree_view(describe_format=DescribeFormat.short)
        self.list_view()

    def describe(self, as_tree=False):
        if as_tree:
            from dbnd._core.constants import DescribeFormat

            self.tree_view(describe_format=DescribeFormat.short)
        else:
            self.ctrl.describe_dag.list_view()
