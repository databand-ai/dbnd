# © Copyright Databand.ai, an IBM Company 2022

from dbnd import Config, parameter
from dbnd._core.constants import CloudType
from dbnd_run.run_settings.env import EnvConfig


class AzureCloudConfig(EnvConfig):
    """Microsoft Azure"""

    _conf__task_family = CloudType.azure
    conn_id = "azure_blob_storage_default"


class AzureCredentialsConfig(Config):
    _conf__task_family = "azure_credentials"

    account_name = parameter(
        description="""The storage account name. This is used to authenticate requests
                signed with an account key and to construct the storage endpoint. It
                is required unless a connection string is given, or if a custom
                domain is used with anonymous authentication."""
    ).none()[str]
    account_key = parameter(
        description="""The storage account key. This is used for shared key authentication.
                If neither account key or sas token is specified, anonymous access
                will be used.""",
        default=None,
        hidden=True,
    ).none()[str]
    sas_token = parameter(
        description="""A shared access signature token to use to authenticate requests
                 instead of the account key. If account key and sas token are both
                 specified, account key will be used to sign. If neither are
                 specified, anonymous access will be used.""",
        default=None,
        hidden=True,
    ).none()[str]
    protocol = parameter(
        description="""The protocol to use for requests. Defaults to https."""
    ).default("https")[str]
    connection_string = parameter(
        description="""If specified, this will override all other parameters besides
                request session. See
                http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
                for the connection string format."""
    ).none()[str]
    endpoint_suffix = parameter(
        description="""The host base component of the url, minus the account name. Defaults
                to Azure (core.windows.net). Override this to use the China cloud
                (core.chinacloudapi.cn)."""
    ).none()[str]
    custom_domain = parameter(
        description="""The custom domain to use. This can be set in the Azure Portal. For
                example, 'www.mydomain.com'."""
    ).none()[str]
    token_credential = parameter(
        description="""A token credential used to authenticate HTTPS requests. The token value
                should be updated before its expiration.""",
        hidden=True,
    ).none()[str]
