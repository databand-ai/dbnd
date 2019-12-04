from dbnd._core.constants import CloudType
from dbnd._core.settings import EnvConfig


class AzureCloudConfig(EnvConfig):
    """Microsoft Azure"""

    _conf__task_family = CloudType.azure
    conn_id = "azure_blob_storage_default"
