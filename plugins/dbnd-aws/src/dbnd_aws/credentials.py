# © Copyright Databand.ai, an IBM Company 2022

import logging

import boto3

from dbnd._core.errors import DatabandConfigError
from dbnd._core.plugin.dbnd_plugins import use_airflow_connections
from dbnd._core.utils.basics.memoized import per_thread_cached


logger = logging.getLogger(__name__)


@per_thread_cached()
def get_boto_session():
    if use_airflow_connections():
        from dbnd_airflow_contrib.credentials_helper_aws import AwsCredentials

        aws_credentials = AwsCredentials()
        logger.debug(
            "getting aws credentials from airflow connection '%s'"
            % aws_credentials.aws_conn_id
        )
        return aws_credentials.get_credentials()[0]
    else:
        logger.debug(
            "getting aws credentials from from environment using boto3 default strategy"
        )
        session = boto3.session.Session()
        if not session.get_credentials():
            raise DatabandConfigError("AWS credentials not found")
        return session


@per_thread_cached()
def get_boto_s3_resource():
    session = get_boto_session()
    return session.resource("s3")


@per_thread_cached()
def get_boto_emr_client(region_name=None):
    return get_boto_session().client("emr", region_name=region_name)
