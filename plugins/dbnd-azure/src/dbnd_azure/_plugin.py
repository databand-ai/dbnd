import dbnd

from dbnd import register_config_cls


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd_azure.env import AzureCloudConfig

    register_config_cls(AzureCloudConfig)
