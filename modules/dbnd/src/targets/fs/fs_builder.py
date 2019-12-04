from targets.fs import FileSystems
from targets.fs.local import LocalFileSystem


_gsc = None
_aws_s3 = None
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


def _cached_gc_credentials():
    global _gsc
    if _gsc:
        return _gsc
    from dbnd_airflow_contrib.credentials_helper_gc import GSCredentials

    _gsc = GSCredentials().get_credentials()
    return _gsc


def _cached_s3_resource():
    global _aws_s3
    if _aws_s3:
        return _aws_s3
    from dbnd_aws.credentials_helper_aws import AwsCredentials

    _aws_s3 = AwsCredentials().get_s3_resource()
    return _aws_s3


def build_fs_client(fs_name):
    if fs_name == FileSystems.s3:
        from targets.fs.s3 import S3Client

        fs = S3Client.from_boto_resource(resource=_cached_s3_resource())
    elif fs_name == FileSystems.gcs:
        from targets.fs.gcs import GCSClient

        fs = GCSClient(
            oauth_credentials=_cached_gc_credentials(), cache_discovery=False
        )
    elif fs_name == FileSystems.azure_blob:
        from targets.fs.azure_blob import AzureBlobStorageClient

        fs = AzureBlobStorageClient(**_cached_azure_blob_credentials())
    elif fs_name == FileSystems.hdfs:
        from dbnd._core.plugin.dbnd_plugins import assert_plugin_enabled

        assert_plugin_enabled("dbnd-hdfs")
        from dbnd_hdfs.fs.hdfs import create_hdfs_client

        fs = create_hdfs_client()

    else:
        fs = LocalFileSystem()

    return fs


_cached_fs = {}


def get_fs_cached(fs_name):
    fs = _cached_fs.get(fs_name)
    if fs:
        return fs

    _cached_fs[fs_name] = fs = build_fs_client(fs_name)
    return fs


def get_fs_cached_for_path(path):
    fs_name = get_fs_client_name(path)
    return get_fs_cached(fs_name)


def get_fs_client_name(path):
    if path.startswith("s3:"):
        return FileSystems.s3
    elif path.startswith("gs:"):
        return FileSystems.gcs
    elif path.startswith("https:") and "blob.core.windows" in path:
        return FileSystems.azure_blob
    elif path.startswith("hdfs:"):
        return FileSystems.hdfs
    return FileSystems.local
