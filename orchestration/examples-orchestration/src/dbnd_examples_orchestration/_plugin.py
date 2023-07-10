# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_examples_orchestration.data import dbnd_examples_data_path

import dbnd

from dbnd import config


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_setup_plugin():
    # register configs
    try:
        config.set_from_config_file(dbnd_examples_data_path("examples_config.cfg"))
    except Exception:
        logger.warning(
            "Could not load examples_config.cfg! Automatic data loading for dbnd-examples is disabled!"
        )
