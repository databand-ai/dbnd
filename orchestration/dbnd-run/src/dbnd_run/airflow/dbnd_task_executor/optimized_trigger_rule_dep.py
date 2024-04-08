# © Copyright Databand.ai, an IBM Company 2022

from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils.db import provide_session
from airflow.utils.state import State


class TriggerRuleDepOptimizied(TriggerRuleDep):
    """
    the original implementation fetching data from DB directly
    we use ti_state_manager with the latest states to calculate that
    Assumption: ti_state_manager is refreshed before we run this code.
    """

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        from dbnd_run.current import try_get_run_executor

        run_executor = try_get_run_executor()

        if not run_executor or not run_executor.task_executor:
            # if we are in Scheduler or Web Server
            # we don't have current run
            # let standard not optimized implementation
            for d in super(TriggerRuleDepOptimizied, self)._get_dep_statuses(
                ti, session, dep_context
            ):
                yield d
            return

        ti_state_manager = run_executor.task_executor.get_ti_state_manager()
        from airflow.utils.trigger_rule import TriggerRule

        TR = TriggerRule

        # Checking that all upstream dependencies have succeeded
        if not ti.task.upstream_list:
            yield self._passing_status(
                reason="The task instance did not have any upstream tasks."
            )
            return

        if ti.task.trigger_rule == TR.DUMMY:
            yield self._passing_status(reason="The task had a dummy trigger rule set.")
            return

        status = ti_state_manager.get_aggregated_state_status(
            dag_id=ti.dag_id,
            execution_date=ti.execution_date,
            task_ids=ti.task.upstream_task_ids,
        )

        successes = status[State.SUCCESS]
        skipped = status[State.SKIPPED]
        failed = status[State.FAILED]
        upstream_failed = status[State.UPSTREAM_FAILED]

        for dep_status in self._evaluate_trigger_rule(
            ti=ti,
            successes=successes,
            skipped=skipped,
            failed=failed,
            upstream_failed=upstream_failed,
            done=successes + skipped + failed + upstream_failed,
            flag_upstream_failed=dep_context.flag_upstream_failed,
            session=session,
        ):
            yield dep_status
