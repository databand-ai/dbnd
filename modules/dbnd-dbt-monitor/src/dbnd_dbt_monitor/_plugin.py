# © Copyright Databand.ai, an IBM Company 2022

import logging

import dbnd


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_get_commands():
    from dbnd_dbt_monitor.multiserver.dbt_multiserver import dbt_monitor

    return [dbt_monitor]
