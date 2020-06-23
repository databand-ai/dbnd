import dbnd

from dbnd import register_config_cls


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd_qubole.qubole_config import QuboleConfig

    register_config_cls(QuboleConfig)
