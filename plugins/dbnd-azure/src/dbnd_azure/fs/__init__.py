AZURE_BLOB_FS_NAME = "azure_blob"

_azure_blob_storage = None


def _cached_azure_blob_credentials():
    global _azure_blob_storage
    if _azure_blob_storage:
        return _azure_blob_storage
    from dbnd_airflow_contrib.credentials_helper_azure_blob import (
        AzureBlobStorageCredentials,
    )

    _azure_blob_storage = AzureBlobStorageCredentials().get_credentials()
    return _azure_blob_storage


def build_azure_blob_fs_client():
    from dbnd_azure.fs.azure_blob import AzureBlobStorageClient

    return AzureBlobStorageClient(**_cached_azure_blob_credentials())


def match_azure_blob_path(path):
    if path.startswith("https:") and "blob.core.windows" in path:
        return AZURE_BLOB_FS_NAME
