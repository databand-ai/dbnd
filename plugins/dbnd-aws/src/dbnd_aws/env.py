from os import environ

from dbnd import parameter
from dbnd._core.constants import CloudType
from dbnd._core.plugin.dbnd_plugins import assert_airflow_enabled
from dbnd._core.settings import EnvConfig


class AwsEnvConfig(EnvConfig):
    """Amazon Web Services"""

    _conf__task_family = CloudType.aws

    conn_id = parameter(
        description="connection id of AWS credentials / region name. "
        "If None,credential boto3 strategy will be used "
        "(http://boto3.readthedocs.io/en/latest/guide/configuration.html)."
    ).value("aws_default")

    region_name = parameter(
        description="region name to use in AWS Hook. "
        "Override the region_name in connection (if provided)"
    ).none[str]

    def prepare_env(self):
        """
        This allows us to use pandas to load remote dataframes directly
        """

        assert_airflow_enabled()

        from dbnd_aws.credentials_helper_aws import AwsCredentials

        boto_session = AwsCredentials(self.conn_id).get_credentials(self.region_name)[0]
        creds = boto_session.get_credentials()

        access_key_env = "AWS_ACCESS_KEY_ID"
        secret_key_env = "AWS_SECRET_ACCESS_KEY"
        if (
            creds.access_key
            and creds.secret_key
            and access_key_env not in environ
            and secret_key_env not in environ
        ):
            environ[access_key_env] = creds.access_key
            environ[secret_key_env] = creds.secret_key
