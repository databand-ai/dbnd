import dbnd

from dbnd import register_config_cls
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled
from targets.fs import FileSystems, register_file_system


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd_luigi.luigi_tracking import register_luigi_tracking

    register_luigi_tracking()
