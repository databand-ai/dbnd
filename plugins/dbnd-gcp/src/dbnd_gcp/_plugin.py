import dbnd

from dbnd import register_config_cls
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled
from dbnd_gcp.fs import build_gcs_client
from targets.fs import FileSystems, register_file_system


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd_gcp.dataflow.dataflow_config import DataflowConfig
    from dbnd_gcp.env import GcpEnvConfig

    register_config_cls(GcpEnvConfig)
    register_config_cls(DataflowConfig)

    if is_plugin_enabled("dbnd-spark"):
        from dbnd_gcp.dataproc.dataproc_config import DataprocConfig

        register_config_cls(DataprocConfig)

    register_file_system(FileSystems.gcs, build_gcs_client)
