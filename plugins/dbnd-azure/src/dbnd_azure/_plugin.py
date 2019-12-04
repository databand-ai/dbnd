import dbnd

from dbnd import register_config_cls


@dbnd.hookimpl
def dbnd_on_pre_init_context(ctx):
    from dbnd_azure.env import AzureCloudConfig

    register_config_cls(AzureCloudConfig)
