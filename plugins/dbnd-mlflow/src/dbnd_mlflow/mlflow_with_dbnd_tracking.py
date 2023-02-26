# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd import config
from dbnd._core.errors import DatabandConfigError

from .url_utils import build_composite_uri, is_composite_uri


logger = logging.getLogger(__name__)

_original_mlflow_tracking_uri = None


def enable_dbnd_for_mlflow_tracking():
    if not config.getboolean("mlflow_tracking", "databand_tracking"):
        return

    try:
        from mlflow import get_tracking_uri, set_tracking_uri
    except ImportError as e:
        logger.warning(
            "An import error has been occurred while trying to import mlflow, "
            "make sure you have it installed in your python environment:\n%s",
            e,
        )
        return

    databand_url = config.get("core", "databand_url")
    if not databand_url:
        logger.info(
            "Although 'databand_tracking' was set in 'mlflow_tracking', "
            "dbnd will not use it since 'core.databand_url' was not set."
        )
        return

    duplicate_tracking_to = config.get("mlflow_tracking", "duplicate_tracking_to")

    if not duplicate_tracking_to:
        duplicate_tracking_to = get_tracking_uri()

        # check if dbnd store uri was already defined with MLFlow config
        if is_composite_uri(duplicate_tracking_to):
            raise DatabandConfigError(
                "Config conflict: MLFlow and DBND configs both define dbnd store uri"
            )

    composite_uri = build_composite_uri(databand_url, duplicate_tracking_to)

    global _original_mlflow_tracking_uri
    _original_mlflow_tracking_uri = get_tracking_uri()
    set_tracking_uri(composite_uri)


def disable_mlflow_tracking(ctx):
    if not config.getboolean("mlflow_tracking", "databand_tracking"):
        return

    try:
        from mlflow import set_tracking_uri
    except ImportError as e:
        logger.warning(
            "An import error has been occurred while trying to import mlflow, "
            "make sure you have it installed in your python environment:\n%s",
            e,
        )
        return

    global _original_mlflow_tracking_uri
    if _original_mlflow_tracking_uri:
        set_tracking_uri(_original_mlflow_tracking_uri)
        _original_mlflow_tracking_uri = None
