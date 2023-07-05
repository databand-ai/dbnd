# © Copyright Databand.ai, an IBM Company 2022

from dbnd import parameter
from dbnd._core.constants import CloudType
from dbnd_run.run_settings.env import EnvConfig


class GcpEnvConfig(EnvConfig):
    """Google Cloud Platform"""

    _conf__task_family = CloudType.gcp
    conn_id = "google_cloud_default"

    delegate_to = parameter.none[str]
