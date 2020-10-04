from os import environ

from dbnd import parameter
from dbnd._core.constants import CloudType
from dbnd._core.settings import EnvConfig
from dbnd_aws.credentials import get_boto_session


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

    update_env_with_boto_creds = parameter(
        description="Update environment of the current process with boto credentials, "
        "so third party libraries like pandas can access s3."
    ).value(False)

    def prepare_env(self):
        """
        This allows us to use pandas to load remote dataframes directly
        """

        if not self.update_env_with_boto_creds:
            return

        boto_session = get_boto_session()
        creds = boto_session.get_credentials()

        access_key_env = "AWS_ACCESS_KEY_ID"
        secret_key_env = "AWS_SECRET_ACCESS_KEY"
        token_key_env = "AWS_SESSION_TOKEN"
        if (
            creds.access_key
            and creds.secret_key
            and access_key_env not in environ
            and secret_key_env not in environ
        ):
            environ[access_key_env] = creds.access_key
            environ[secret_key_env] = creds.secret_key
            if creds.token:
                environ[token_key_env] = creds.token
