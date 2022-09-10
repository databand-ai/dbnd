# © Copyright Databand.ai, an IBM Company 2022

import logging

import dbnd


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_get_commands():
    from dbnd_datastage_monitor.multiserver.datastage_multiserver import (
        datastage_monitor,
    )

    return [datastage_monitor]
