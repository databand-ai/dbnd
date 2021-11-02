import logging

from dbnd._core.constants import TaskRunState
from dbnd._core.errors.base import DatabandRunError, DatabandSigTermError
from dbnd._core.task_ctrl.task_dag import topological_sort
from dbnd._core.task_executor.task_executor import TaskExecutor


logger = logging.getLogger(__name__)


class LocalTaskExecutor(TaskExecutor):
    def do_run(self):
        topological_tasks = topological_sort([tr.task for tr in self.task_runs])
        fail_fast = self.settings.run.fail_fast
        task_failed = False

        task_runs_to_update_state = []
        for task in topological_tasks:
            tr = self.run.get_task_run_by_id(task.task_id)
            if tr.is_reused:
                continue

            if fail_fast and task_failed:
                logger.info(
                    "Setting %s to %s", task.task_id, TaskRunState.UPSTREAM_FAILED
                )
                tr.set_task_run_state(TaskRunState.UPSTREAM_FAILED, track=False)
                task_runs_to_update_state.append(tr)
                continue

            upstream_task_runs = [
                self.run.get_task_run_by_id(t.task_id)
                for t in task.ctrl.task_dag.upstream
            ]
            failed_upstream = [
                upstream_task_run
                for upstream_task_run in upstream_task_runs
                if upstream_task_run.task_run_state in TaskRunState.fail_states()
            ]
            if failed_upstream:
                logger.info(
                    "Setting %s to %s", task.task_id, TaskRunState.UPSTREAM_FAILED
                )
                tr.set_task_run_state(TaskRunState.UPSTREAM_FAILED, track=False)
                task_runs_to_update_state.append(tr)
                continue

            if self.run.is_killed():
                logger.info(
                    "Databand Context is killed! Stopping %s to %s",
                    task.task_id,
                    TaskRunState.FAILED,
                )
                tr.set_task_run_state(TaskRunState.FAILED, track=False)
                task_runs_to_update_state.append(tr)
                continue

            logger.debug("Executing task: %s", task.task_id)

            try:
                tr.runner.execute()
            except DatabandSigTermError as e:
                raise e
            except Exception:
                task_failed = True
                logger.exception("Failed to execute task '%s':" % task.task_id)

        if task_runs_to_update_state:
            self.run.tracker.set_task_run_states(task_runs_to_update_state)

        if task_failed:
            err = _collect_errors(self.run.task_runs)

            if err:
                raise DatabandRunError(err)


def _collect_errors(task_runs):
    err = ""
    upstream_failed = []
    failed = []
    for task_run in task_runs:
        task_name = task_run.task.task_name
        if task_run.task_run_state == TaskRunState.UPSTREAM_FAILED:
            # we don't want to show upstream failed in the list
            upstream_failed.append(task_name)
        elif task_run.task_run_state in TaskRunState.direct_fail_states():
            failed.append(task_name)
    if upstream_failed:
        err += "Task that didn't run because of failed dependency:\n\t{}\n".format(
            "\n\t".join(upstream_failed)
        )
    if failed:
        err += "Failed tasks are:\n\t{}".format("\n\t".join(failed))
    return err
