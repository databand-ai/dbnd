_aws_s3 = None


def _cached_s3_resource():
    global _aws_s3
    if _aws_s3:
        return _aws_s3
    from dbnd_aws.credentials_helper_aws import AwsCredentials

    _aws_s3 = AwsCredentials().get_s3_resource()
    return _aws_s3


def build_s3_fs_client():
    from dbnd_aws.fs.s3 import S3Client

    return S3Client.from_boto_resource(resource=_cached_s3_resource())
