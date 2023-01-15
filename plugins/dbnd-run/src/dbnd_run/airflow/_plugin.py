# © Copyright Databand.ai, an IBM Company 2022

import logging

import dbnd


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd import register_config_cls
    from dbnd_run.airflow.config import AirflowConfig

    register_config_cls(AirflowConfig)
