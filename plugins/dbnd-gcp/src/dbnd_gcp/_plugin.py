import dbnd

from dbnd import register_config_cls
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled


@dbnd.hookimpl
def dbnd_on_pre_init_context(ctx):
    from dbnd_gcp.dataflow.dataflow_config import DataflowConfig
    from dbnd_gcp.env import GcpEnvConfig

    register_config_cls(GcpEnvConfig)
    register_config_cls(DataflowConfig)

    if is_plugin_enabled("dbnd-spark"):
        from dbnd_gcp.dataproc.dataproc_config import DataprocConfig

        register_config_cls(DataprocConfig)
