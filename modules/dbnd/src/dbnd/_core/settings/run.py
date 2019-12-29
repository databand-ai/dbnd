import logging

from datetime import datetime
from typing import List, Optional

from dbnd._core.constants import TaskExecutorType
from dbnd._core.errors import DatabandConfigError
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.plugin.dbnd_plugins import assert_airflow_enabled, is_airflow_enabled
from dbnd._core.task import config
from dbnd._core.task_executor.local_task_executor import LocalTaskExecutor


logger = logging.getLogger(__name__)


class RunConfig(config.Config):
    """Databand's per run settings (e.g. execution date)"""

    _conf__task_family = "run"

    # on none generated at DatabandRun
    name = parameter.value(default=None, description="Specify run name")[str]

    description = parameter.value(default=None, description="Specify run description")[
        Optional[str]
    ]

    parallel = parameter(default=None)[bool]
    task_executor_type = parameter(
        default=None,
        description="Alternate executor type: "
        " local/airflow_inprocess/airflow_multiprocess_local/airflow_kubernetes,"
        "  see docs for more options",
    )[str]

    submit_driver = parameter(
        description="override env.submit_driver for specific environment"
    ).none[bool]
    submit_tasks = parameter(
        description="override env.submit_tasks for specific environment"
    ).none[bool]

    enable_airflow_kubernetes = parameter(
        default=True,
        description="Enable use of kubernetes executor for kubebernetes engine submission",
    )[bool]

    execution_date = parameter(default=None, description="Override execution date")[
        datetime
    ]

    # Execution specific
    id = parameter(default=None, description="The list of task ids to run")[List[str]]
    task = parameter(
        default=None, description="Run only specified tasks (regular expresion)"
    )[str]

    ignore_dependencies = parameter(
        description="The regex to filter specific task_ids"
    ).value(False)
    ignore_first_depends_on_past = parameter(
        description="The regex to filter specific task_ids"
    ).value(False)

    pool = parameter(default=None, description="Resource pool to use")[str]

    donot_pickle = parameter(
        description="Do not attempt to pickle the DAG object to send over "
        "to the workers, just tell the workers to run their version "
        "of the code."
    ).value(False)

    mark_success = parameter(
        description="Mark jobs as succeeded without running them"
    ).value(False)
    skip_completed = parameter(
        description="Mark jobs as succeeded without running them"
    ).value(True)
    fail_fast = parameter(
        description="Skip all remaining tasks if a task has failed"
    ).value(True)
    enable_prod = parameter(description="Enable production tasks").value(False)
    is_archived = parameter(description="Save this run in the archive").value(False)

    heartbeat_interval_s = parameter(
        description="How often a run should send a heartbeat to the server. Set -1 to disable"
    )[int]
    heartbeat_timeout_s = parameter(
        description="How old can a run's last heartbeat be before we consider it failed. Set -1 to disable"
    )[int]

    enable_concurent_sqlite = parameter(
        description="Enable concurrent execution with sqlite db (use only for debug!)"
    ).value(False)

    interactive = parameter(
        default=False,
        description="When submitting driver to remote execution keep tracking of submitted process and wait for completion",
    )[bool]

    def _validate(self):
        super(RunConfig, self)._validate()
        if self.task_executor_type is None:
            if is_airflow_enabled():
                from dbnd_airflow.executors import AirflowTaskExecutorType

                self.task_executor_type = AirflowTaskExecutorType.airflow_inprocess
            else:
                self.task_executor_type = TaskExecutorType.local

        if self.parallel:
            if is_airflow_enabled():
                from dbnd_airflow.executors import AirflowTaskExecutorType

                if self.task_executor_type == TaskExecutorType.local:
                    logger.warning(
                        "Auto switching to engine type '%s' due to parallel mode.",
                        AirflowTaskExecutorType.airflow_multiprocess_local,
                    )
                    self.task_executor_type = (
                        AirflowTaskExecutorType.airflow_multiprocess_local
                    )
                if self.task_executor_type == AirflowTaskExecutorType.airflow_inprocess:
                    logger.warning(
                        "Auto switching to engine type '%s' due to parallel mode.",
                        AirflowTaskExecutorType.airflow_multiprocess_local,
                    )
                    self.task_executor_type = (
                        AirflowTaskExecutorType.airflow_multiprocess_local
                    )
            else:
                logger.warning(
                    "Airflow is not installed, parallel mode is not supported"
                )

    def get_task_executor(
        self, run, task_executor_type, host_engine, target_engine, task_runs
    ):
        if task_executor_type == TaskExecutorType.local:
            executor_cls = LocalTaskExecutor
        elif task_executor_type.startswith("airflow"):
            assert_airflow_enabled()
            from dbnd_airflow.dbnd_task_executor.dbnd_task_executor_via_airflow import (
                AirflowTaskExecutor,
            )

            executor_cls = AirflowTaskExecutor
        else:
            raise DatabandConfigError(
                "Unsupported engine type %s" % self.task_executor_type
            )

        return executor_cls(
            run,
            task_executor_type=task_executor_type,
            host_engine=host_engine,
            target_engine=target_engine,
            task_runs=task_runs,
        )
