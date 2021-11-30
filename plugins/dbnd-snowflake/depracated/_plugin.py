import logging

import dbnd


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd import register_config_cls
    from dbnd_snowflake.snowflake_config import SnowflakeConfig

    register_config_cls(SnowflakeConfig)
