# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import config


class SchedulerConfig(config.Config):
    """(Advanced) Databand's scheduler"""

    _conf__task_family = "scheduler"

    config_file = parameter(
        default=None,
        description="Set a path to the file defining scheduled jobs to execute.",
    )[str]

    # by default the scheduler drop-in will decide whether to sync or not based on if it's running inside the scheduler or not (and not the websrver)
    # the next two params can be used to force it one way or the other
    never_file_sync = parameter(
        default=False,
        description="Disable syncing the scheduler config_file to the database.",
    )[bool]

    always_file_sync = parameter(
        default=False,
        description="Enable forcibly syncing the scheduler config_file to the database.",
    )[bool]

    no_ui_cli_edit = parameter(
        default=False,
        description="Disable creating, editing, and deleting scheduled jobs from the CLI and UI. Scheduled job "
        "definitions will only be taken from the scheduler config file.",
    )

    refresh_interval = parameter(
        default=1,
        description="Set the interval to refresh the scheduled job list from the db and/or a config file",
    )[int]

    active_by_default = parameter(
        default=True,
        description="Determine whether new scheduled jobs will be activated by default.",
    )[bool]

    default_retries = parameter(
        description="Set the number of times to retry a failed run, "
        "unless set to a different value on the scheduled job"
    )[int]

    shell_cmd = parameter(
        description="If shell_cmd is True, the specified command will be executed through the shell. "
        "This can be useful if you are using Python primarily "
        "for the enhanced control flow it offers "
        "over most system shells and still want convenient access to other shell features "
        "such as shell pipes, filename wildcards, environment variable expansion, "
        "and expansion of ~ to a user's home directory."
    )[bool]
