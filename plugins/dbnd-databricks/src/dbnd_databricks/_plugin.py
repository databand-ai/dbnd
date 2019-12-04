import dbnd

from dbnd import register_config_cls


@dbnd.hookimpl
def dbnd_on_pre_init_context(ctx):
    from dbnd_databricks.databrick_config import DatabricksConfig

    register_config_cls(DatabricksConfig)
