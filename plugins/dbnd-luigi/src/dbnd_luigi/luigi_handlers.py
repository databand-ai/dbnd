import logging

from dbnd._core.constants import TaskRunState
from dbnd_luigi.luigi_postgres import (
    handle_postgres_histogram_logging,
    should_log_pg_histogram,
)
from dbnd_luigi.luigi_run_manager import LuigiRunManager


logger = logging.getLogger(__name__)


class LuigiEventsHandler:
    def __init__(self):
        self.run_manager: LuigiRunManager = None

    def set_run_manager(self, run_manager):
        self.run_manager = run_manager

    def on_run_start(self, luigi_task):
        # implicitly creates dbnd context if not exists
        dbnd_task = self.run_manager.get_dbnd_task(luigi_task)
        self.run_manager.start_task(dbnd_task)

    def on_success(self, luigi_task):
        if should_log_pg_histogram(luigi_task):
            handle_postgres_histogram_logging(luigi_task)

        dbnd_task = self.run_manager.get_dbnd_task(luigi_task)
        self.run_manager.finish_task(dbnd_task, TaskRunState.SUCCESS)

    def on_failure(self, luigi_task, exc):
        from dbnd._core.task_run.task_run_error import TaskRunError

        dbnd_task = self.run_manager.get_dbnd_task(luigi_task)
        task_run_error = TaskRunError.build_from_ex(exc, dbnd_task.current_task_run)

        self.run_manager.finish_task(dbnd_task, TaskRunState.FAILED, err=task_run_error)

    def on_dependency_discovered(self, father_task, child_task):
        """
        Dependency will be run.
        This event is triggered before "START" is triggered
        """
        logger.info(
            "Dependency discovered: %s - > %s ", father_task.task_id, child_task.task_id
        )

        father_dbnd_task = self.run_manager.get_dbnd_task(father_task)
        self.run_manager.encounter_task(father_dbnd_task)

        child_dbnd_task = self.run_manager.get_dbnd_task(child_task)

        father_dbnd_task.set_upstream(child_dbnd_task)
        father_dbnd_task.task_meta.add_child(child_dbnd_task.task_id)

    def on_dependency_present(self, luigi_task):
        """
        Dependency is already found. This could either mean that dependency is complete, or was already found by Luigi
        """
        logger.warning("Dependency is present: %s", luigi_task.task_id)

        dbnd_task = self.run_manager.get_dbnd_task(luigi_task)
        dbnd_task._luigi_task_completed = True

        self.run_manager.encounter_task(dbnd_task)

    def on_dependency_missing(self, luigi_task):
        logger.warning(
            "Encountered missing dependency for task {0}".format(luigi_task.task_id)
        )

        dbnd_task = self.run_manager.get_dbnd_task(luigi_task)
        self._mark_downstream_dependency_missing(dbnd_task)

    def _mark_downstream_dependency_missing(self, task):
        self.run_manager.add_missing_dep(task)
        for down_task in task.task_dag.downstream:
            self._mark_downstream_dependency_missing(down_task)
