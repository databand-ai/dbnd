from typing import List

from dbnd import parameter
from dbnd._core.task import Config


class TrackingConfig(Config):
    _conf__task_family = "tracking"

    local_dbnd_java_agent = parameter(
        default=None,
        description="A dbnd java agent jar which used to track a java application, located on the local machine",
    )[str]

    databricks_dbnd_java_agent = parameter(
        default=None,
        description="A dbnd java agent jar which used to track a java application, located on remote machine",
    )[str]

    dbnd_tracking_packages = parameter(
        default=None,
        description="A List of packages to track inside the jar application",
    )[List[str]]
