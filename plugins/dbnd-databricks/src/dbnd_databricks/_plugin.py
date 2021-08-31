import dbnd

from dbnd import register_config_cls


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd_databricks.databricks_config import DatabricksConfig

    register_config_cls(DatabricksConfig)
