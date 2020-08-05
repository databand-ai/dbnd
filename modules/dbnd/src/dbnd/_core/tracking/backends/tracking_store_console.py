import logging
import typing

from datetime import timedelta

import six

from dbnd._core.constants import TaskRunState
from dbnd._core.current import is_verbose
from dbnd._core.tracking.backends import TrackingStore
from dbnd._core.tracking.schemas.metrics import Metric
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor.ascii_graph import Pyasciigraph


if typing.TYPE_CHECKING:
    from typing import List
    from dbnd._core.task_run.task_run import TaskRun
logger = logging.getLogger(__name__)


class ConsoleStore(TrackingStore):
    def __init__(self):
        super(ConsoleStore, self).__init__()
        self.max_log_value_len = 50
        self.verbose = is_verbose()
        self.ascii_graph = Pyasciigraph()

    def init_scheduled_job(self, scheduled_job, update_existing):
        super(ConsoleStore, self).init_scheduled_job(scheduled_job, update_existing)

    def init_run(self, run):
        logger.info(
            run.describe.run_banner(
                "Running Databand!", color="cyan", show_run_info=True
            )
        )

        if run.context.name == "interactive":
            from dbnd._core.tools import ipython

            ipython.show_run_url(run.run_url)

    def set_task_reused(self, task_run):
        task = task_run.task
        logger.info(
            task.ctrl.visualiser.banner(
                "Task %s has been completed already!" % task.task_id,
                "magenta",
                task_run=task_run,
            )
        )

    def set_task_run_state(self, task_run, state, error=None, timestamp=None):
        super(ConsoleStore, self).set_task_run_state(task_run=task_run, state=state)
        task = task_run.task

        # optimize, don't print success banner for fast running tasks
        quick_task = task_run.finished_time and (
            task_run.finished_time
            - (task_run.start_time if task_run.start_time else utcnow())
        ) < timedelta(seconds=5)
        show_simple_log = not self.verbose and (
            task_run.task.task_is_system or quick_task
        )
        level = logging.INFO
        color = "cyan"
        task_friendly_id = task_run.task_af_id
        if state in [TaskRunState.RUNNING, TaskRunState.QUEUED]:
            task_msg = "Running task %s" % task_friendly_id
        elif state == TaskRunState.SUCCESS:
            task_msg = "Task %s has been completed!" % (task_friendly_id)
            color = "green"
        elif state == TaskRunState.FAILED:
            task_msg = "Task %s has failed!" % (task_friendly_id)
            color = "red"
            level = logging.ERROR
            if task_run.task.get_task_family() != "_DbndDriverTask":
                show_simple_log = False
        elif state == TaskRunState.CANCELLED:
            task_msg = "Task %s has been canceled!" % (task_friendly_id)
            color = "red"
            level = logging.ERROR
        else:
            task_msg = "Task %s at %s state" % (task_friendly_id, state)

        if show_simple_log:
            logger.log(level, task_msg)
            return

        try:
            logger.log(
                level, task.ctrl.visualiser.banner(task_msg, color, task_run=task_run)
            )
        except Exception as ex:
            logger.log(level, "%s \nfailed to create banner: %s" % (task_msg, ex))

    def add_task_runs(self, run, task_runs):
        pass

    def save_external_links(self, task_run, external_links_dict):
        task = task_run.task
        attempt_uid = task_run.task_run_attempt_uid

        logger.info(
            "%s %s has URLs to external resources: \n\t%s",
            task,
            attempt_uid,
            "\t\n".join(
                "%s: %s" % (k, v) for k, v in six.iteritems(external_links_dict)
            ),
        )

    def is_ready(self):
        return True

    def log_histograms(self, task_run, key, histogram_spec, value_meta, timestamp):
        for hist_name, hist_values in value_meta.histograms.items():
            for line in self.ascii_graph.graph(
                "Histogram logged: {}.{}".format(key, hist_name),
                list(zip(*reversed(hist_values))),
            ):
                logger.info(line)
        # TODO: Add more compact logging if user opts out

    def log_metrics(self, task_run, metrics):
        # type: (TaskRun, List[Metric]) -> None
        logger.info("Metrics logged: {}".format([m.key for m in metrics]))
