# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import datetime
from typing import List, Optional

from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import config


logger = logging.getLogger(__name__)


class RunConfig(config.Config):
    """Databand's per run settings (e.g. execution date)"""

    _conf__task_family = "run"

    ######
    # on none generated at DatabandRun
    name = parameter.value(default=None, description="Specify the run's name.")[str]

    description = parameter.value(
        default=None, description="Specify the run's description"
    )[Optional[str]]

    # Executor configuration
    parallel = parameter(default=None).help("Run specific tasks in parallel.")[bool]
    task_executor_type = parameter(
        default=None,
        description="Set alternate executor type. Some of the options are `local`, `airflow_inprocess`, "
        "`airflow_multiprocess_local`, or `airflow_kubernetes`.",
    )[str]

    enable_airflow_kubernetes = parameter(
        default=True,
        description="Enable the use of Kubernetes executor for kubernetes engine submission.",
    )[bool]

    ######
    # Local/Remote control
    interactive = parameter(
        default=False,
        description="When submitting a driver to remote execution, keep track of the "
        "submitted process and wait for completion.",
    )[bool]
    submit_driver = parameter(
        description="Override `env.submit_driver` for the specific environment."
    ).none[bool]
    submit_tasks = parameter(
        description="Override `env.submit_tasks` for the specific environment."
    ).none[bool]

    # What to do on run
    open_web_tracker_in_browser = parameter(
        description="If True, opens web tracker in browser during the task's run."
    ).value(False)

    is_archived = parameter(
        description="Determine whether to save this run in the archive."
    ).value(False)

    dry = parameter(default=False).help(
        "Do not execute tasks, stop before sending them to the execution, and print their status."
    )[bool]

    run_result_json_path = parameter(default=None).help(
        "Set the path to save the task band of the run."
    )[str]

    debug_pydevd_pycharm_port = parameter(default=None).help(
        "Enable debugging with `pydevd_pycharm` by setting this to the port value expecting the debugger to connect. \n"
        "This will start a new `settrace` connecting to `localhost` on the requested port, "
        "right before starting the driver task_run."
    )[int]

    ######
    # AIRFLOW EXECUTOR CONFIG
    execution_date = parameter(
        default=None, description="Override the run's execution date."
    )[datetime]
    mark_success = parameter(
        description="Mark jobs as succeeded without running them."
    ).value(False)

    ######
    # Task Selectors (to schedule specific task from pipeline)
    id = parameter(default=None, description="Set the list of task IDs to run.")[
        List[str]
    ]
    selected_tasks_regex = parameter(
        default=None,
        description="Run only specified tasks. This is a regular expression.",
    )[List[str]]

    ignore_dependencies = parameter(
        description="The regex to filter specific task_ids."
    ).value(False)
    ignore_first_depends_on_past = parameter(
        description="The regex to filter specific task_ids."
    ).value(False)

    ######
    # Scheduler configuration

    skip_completed = parameter(
        description="Mark jobs as succeeded without running them."
    ).value(True)
    fail_fast = parameter(
        description="Skip all remaining tasks if a task has failed."
    ).value(True)
    enable_prod = parameter(description="Enable production tasks.").value(False)

    skip_completed_on_run = parameter(default=True).help(
        "Determine whether dbnd task should check that the task is completed and mark it as re-used on task execution."
    )[bool]

    validate_task_inputs = parameter(default=True).help(
        "Determine whether dbnd task should check that all input files exist."
    )[bool]

    validate_task_outputs = parameter(default=True).help(
        "Determine whether dbnd task should check that all outputs exist after the task has been executed."
    )[bool]

    validate_task_outputs_on_build = parameter(default=False).help(
        "Determine whether dbnd task should check that there are no incomplete outputs before the task executes."
    )[bool]

    pipeline_band_only_check = parameter(default=False).help(
        "When checking if the pipeline is completed, check only if the band file exists, and skip the tasks."
    )[bool]

    recheck_circle_dependencies = parameter(
        description="Recheck circle dependencies on every task creation, "
        "use it if you need to find a circle in your graph."
    ).value(False)

    task_complete_parallelism_level = parameter(default=1).help(
        "Set the number of threads to use when checking if tasks are already complete."
    )[int]

    pool = parameter(
        default=None, description="Determine which resource pool will be used."
    )[str]

    ######
    # Advanced Run settings (debug/workarounds)
    # run .pickle file
    always_save_pipeline = parameter(
        description="Enable always saving pipeline to pickle."
    ).value(False)
    disable_save_pipeline = parameter(description="Disable pipeline pickling.").value(
        False
    )
    donot_pickle = parameter(
        description="Do not attempt to pickle the DAG object to send over to the workers. "
        "Instead, tell the workers to run their version of the code."
    ).value(False)
    pickle_handler = parameter(
        default=None,
        description="Define a python pickle handler to be used to pickle the run's data",
    )[str]
    enable_concurent_sqlite = parameter(
        description="Enable concurrent execution with sqlite database. This should only be used for debugging purposes."
    ).value(False)

    ######
    # HEARTBEAT (process that updates on driver status every `heartbeat_interval_s`
    #
    heartbeat_interval_s = parameter(
        description="Set how often a run should send a heartbeat to the server. "
        "Setting this to -1 will disable it altogether."
    )[int]
    heartbeat_timeout_s = parameter(
        description="Set how old a run's last heartbeat can be before we consider it to have failed."
        "Setting this to -1 will disable this."
    )[int]
    heartbeat_sender_log_to_file = parameter(
        description="Enable creating a separate log file for the heartbeat sender and don't log the run process stdout."
    )[bool]

    hearbeat_disable_plugins = parameter(
        default=False, description="Disable dbnd plugins at heartbeat sub-process."
    )[bool]
    ######
    # Task/Pipeline in task Execution
    task_run_at_execution_time_enabled = parameter(
        default=True, description="Allow tasks calls during another task's execution."
    )[bool]
    task_run_at_execution_time_in_memory_outputs = parameter(
        default=False,
        description="Enable storing outputs for an inline task at execution time in memory. (do not use FileSystem)",
    )[bool]
    target_cache_on_access = parameter(
        default=True,
        description="Enable caching target values in memory during execution.",
    )[bool]
