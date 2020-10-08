from dbnd_azure.credentials import get_azure_credentials


AZURE_BLOB_FS_NAME = "azure_blob"


def build_azure_blob_fs_client():
    from dbnd_azure.fs.azure_blob import AzureBlobStorageClient

    return AzureBlobStorageClient(**get_azure_credentials())


def match_azure_blob_path(path):
    if path.startswith("https:") and "blob.core.windows" in path:
        return AZURE_BLOB_FS_NAME
