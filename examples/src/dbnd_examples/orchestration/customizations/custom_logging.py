# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd import get_databand_context, new_dbnd_context, task


logger = logging.getLogger(__name__)


def custom_logging_mod():
    base_config = get_databand_context().settings.log.get_dbnd_logging_config_base()

    base_config["handlers"]["my_file"] = {
        "class": "logging.FileHandler",
        "formatter": "formatter",
        "filename": "/tmp/my_custom_file",
        "encoding": "utf-8",
    }
    base_config["root"]["handlers"].append("my_file")

    return base_config


@task
def task_with_some_logging():
    logging.info("root logger")
    logger.info("custom logger")
    logger.warning("warning!")
    return "ok"


if __name__ == "__main__":
    mod_func = "dbnd_examples.feature_system.custom_logging.custom_logging_mod"
    with new_dbnd_context(conf={"log": {"custom_dict_config": mod_func}}):
        task_with_some_logging.dbnd_run()
