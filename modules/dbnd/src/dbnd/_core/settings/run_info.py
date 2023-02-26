# © Copyright Databand.ai, an IBM Company 2022

import logging
import subprocess
import sys

from datetime import datetime
from os import environ
from typing import Optional

from dbnd._core.configuration.environ_config import (
    ENV_DBND__ENV_IMAGE,
    ENV_DBND__ENV_MACHINE,
)
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import config
from dbnd._core.tracking.schemas.tracking_info_objects import TaskRunEnvInfo
from dbnd._core.utils.basics.text_banner import safe_string
from dbnd._core.utils.platform.windows_compatible.getuser import dbnd_getuser
from dbnd._core.utils.project.project_fs import project_path
from dbnd._core.utils.timezone import utcnow
from dbnd._core.utils.uid_utils import get_uuid
from targets.values.version_value import VersionStr


logger = logging.getLogger(__name__)


def get_task_info_cmd():
    return subprocess.list2cmdline(sys.argv)


class RunInfoConfig(config.Config):
    """(Advanced) Databand's run information gatherer"""

    _conf__task_family = "run_info"

    ######
    # on none generated at DatabandRun
    name = parameter.value(default=None, description="Specify the run's name.")[str]

    description = parameter.value(
        default=None, description="Specify the run's description"
    )[Optional[str]]

    execution_date = parameter(
        default=None, description="Override the run's execution date."
    )[datetime]

    is_archived = parameter(
        description="Determine whether to save this run in the archive."
    ).value(False)

    source_version = parameter(
        default="git", description="gather version control via git/None"
    )[VersionStr]
    user_data = parameter(default=None, description="UserData")[str]
    user = parameter(default=None, description="override user name with the value")[str]

    def build_task_run_info(self):
        task_run_env_uid = get_uuid()
        import dbnd

        logger.debug("Created new task run env with uid '%s'", task_run_env_uid)

        machine = environ.get(ENV_DBND__ENV_MACHINE, "")
        if environ.get(ENV_DBND__ENV_IMAGE, None):
            machine += " image=%s" % environ.get(ENV_DBND__ENV_IMAGE)
        return TaskRunEnvInfo(
            uid=task_run_env_uid,
            databand_version=dbnd.__version__,
            user_code_version=self.source_version,
            user_code_committed=True,
            cmd_line=get_task_info_cmd(),
            user=self.user or dbnd_getuser(),
            machine=machine,
            project_root=project_path(),
            user_data=safe_string(self.user_data, max_value_len=500),
            heartbeat=utcnow(),
        )
