# © Copyright Databand.ai, an IBM Company 2022

from airflow.contrib.hooks.aws_hook import AwsHook


class AwsCredentials(AwsHook):
    def __init__(self, aws_conn_id="aws_default"):
        super(AwsCredentials, self).__init__(aws_conn_id)

    def get_credentials(self, region_name=None):
        return self._get_credentials(region_name)

    def get_s3_resource(self, region_name=None):
        return self.get_resource_type(resource_type="s3", region_name=region_name)
