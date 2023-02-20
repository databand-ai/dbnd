# © Copyright Databand.ai, an IBM Company 2022

from dbnd_aws.credentials import get_boto_s3_resource


def build_s3_fs_client():
    from dbnd_aws.fs.s3 import S3Client

    return S3Client.from_boto_resource(resource=get_boto_s3_resource())
