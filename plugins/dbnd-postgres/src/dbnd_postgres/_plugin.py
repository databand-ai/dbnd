from dbnd_postgres.postgres_config import PostgresConfig

import dbnd

from dbnd import register_config_cls


@dbnd.hookimpl
def dbnd_setup_plugin():
    register_config_cls(PostgresConfig)
