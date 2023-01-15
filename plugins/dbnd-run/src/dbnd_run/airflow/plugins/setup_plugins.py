# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import logging
import os

from dbnd._core.utils.basics.format_exception import format_exception_as_str
from dbnd_run.airflow.utils import dbnd_airflow_path


logger = logging.getLogger(__name__)


def setup_scheduled_dags(sub_dir=None, unlink_first=True):
    from airflow import settings

    sub_dir = sub_dir or settings.DAGS_FOLDER

    if not sub_dir:
        raise Exception("Can't link scheduler: airflow's dag folder is undefined")

    source_path = dbnd_airflow_path("scheduler", "dags", "dbnd_dropin_scheduler.py")
    target_path = os.path.join(sub_dir, "dbnd_dropin_scheduler.py")

    if unlink_first and os.path.islink(target_path):
        try:
            logger.info("unlinking existing drop-in scheduler file at %s", target_path)
            os.unlink(target_path)
        except Exception:
            logger.error(
                "failed to unlink drop-in scheduler file at %s: %s",
                target_path,
                format_exception_as_str(),
            )
            return

    if not os.path.exists(target_path):
        try:
            logger.info("Linking %s to %s.", source_path, target_path)
            if not os.path.exists(sub_dir):
                os.makedirs(sub_dir)
            os.symlink(source_path, target_path)
        except Exception:
            logger.error(
                "failed to link drop-in scheduler in the airflow dags_folder: %s",
                format_exception_as_str(),
            )
