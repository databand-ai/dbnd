from datetime import datetime
from typing import List, Optional

from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import config


class RunConfig(config.Config):
    """Databand's per run settings (e.g. execution date)"""

    _conf__task_family = "run"

    # on none generated at DatabandRun
    name = parameter.value(default=None, description="Specify run name")[str]

    description = parameter.value(default=None, description="Specify run description")[
        Optional[str]
    ]

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
