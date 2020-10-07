import dbnd

from dbnd import register_config_cls
from dbnd_postgres.postgres_config import PostgresConfig


@dbnd.hookimpl
def dbnd_setup_plugin():
    register_config_cls(PostgresConfig)
