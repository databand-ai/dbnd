# © Copyright Databand.ai, an IBM Company 2022

# PLEASE DO NOT MOVE/RENAME THIS FILE, IT'S SERIALIZED INTO AIRFLOW DB

import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from dbnd._core.configuration.dbnd_config import config as dbnd_config


logger = logging.getLogger(__name__)


class DbndOperator(BaseOperator):
    """
    This is the Airflow operator that is created for every Databand Task
    """

    ui_color = "#ffefeb"

    @apply_defaults
    def __init__(self, dbnd_task_type, dbnd_task_id, **kwargs):
        super(DbndOperator, self).__init__(**kwargs)
        self._task_type = dbnd_task_type
        self.dbnd_task_id = dbnd_task_id
        # Make sure that we run in separate pool
        self.pool = dbnd_config.get("airflow", "dbnd_pool")

    @property
    def task_type(self):
        # we want to override task_type so we can have unique types for every Databand task
        v = getattr(self, "_task_type", None)
        if v:
            return v
        return BaseOperator.task_type.fget(self)

    @property
    def retry_delay(self):
        """
        This property is called upon when airflow tries to calculate the retry delay for a task.
        If we are executing kubernetes pods we need to update the retry delay to our configuration settings.
        Otherwise we are returning the same value that DbndOperator would return normally - task_run_executor.task.retry_delay
        """
        from dbnd_run.airflow.dbnd_task_executor.dbnd_execute import (
            dbnd_operator__get_task_retry_delay,
        )

        return dbnd_operator__get_task_retry_delay(self)

    @retry_delay.setter
    def retry_delay(self, value):
        """
        Maintain airflow's way of living
        """
        self._retry_delay = value

    def execute(self, context):
        logger.debug("Running dbnd task from airflow operator %s", self.task_id)
        from dbnd_run.airflow.dbnd_task_executor.dbnd_execute import (
            dbnd_operator__execute,
        )

        return dbnd_operator__execute(self, context)

    def on_kill(self):
        from dbnd_run.airflow.dbnd_task_executor.dbnd_execute import dbnd_operator__kill

        return dbnd_operator__kill(self)

    @property
    def deps(self):
        """
        Returns the list of dependencies for the operator. These differ from execution
        context dependencies in that they are specific to tasks and can be
        extended/overridden by subclasses.
        """
        from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
        from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep

        try:
            from dbnd_run.airflow.dbnd_task_executor import (
                TriggerRuleDepOptimizied as TriggerRuleDep,
            )
        except Exception:
            from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep

        return {
            NotInRetryPeriodDep(),
            PrevDagrunDep(),
            TriggerRuleDep(),  # PATCH: We replace TriggerRuleDep with TriggerRuleDepOptimizied
        }
