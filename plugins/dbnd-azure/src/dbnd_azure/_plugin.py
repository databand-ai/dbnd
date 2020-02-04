import dbnd

from dbnd import register_config_cls
from dbnd_azure.fs import (
    AZURE_BLOB_FS_NAME,
    build_azure_blob_fs_client,
    match_azure_blob_path,
)
from targets.fs import register_file_system, register_file_system_name_custom_resolver


@dbnd.hookimpl
def dbnd_setup_plugin():
    register_file_system(AZURE_BLOB_FS_NAME, build_azure_blob_fs_client)
    register_file_system_name_custom_resolver(match_azure_blob_path)

    from dbnd_azure.env import AzureCloudConfig

    register_config_cls(AzureCloudConfig)
