# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd import get_databand_context
from dbnd._core.utils.basics.memoized import cached

from .url_utils import parse_composite_uri


logger = logging.getLogger(__name__)


@cached()
def get_dbnd_store(store_uri=None, artifact_uri=None):
    dbnd_store_url, duplicate_tracking_to = parse_composite_uri(store_uri)

    logger.info("MLFlow DBND Tracking Store url: {}".format(dbnd_store_url))
    logger.info(
        "MLFlow DBND Tracking Store duplication to: {}".format(duplicate_tracking_to)
    )

    dbnd_store = get_databand_context().tracking_store

    duplication_store = None
    if duplicate_tracking_to is not None:
        # avoid cyclic imports during `_tracking_store_registry.register_entrypoints()`
        try:
            from mlflow.tracking import _get_store
        except ImportError as e:
            logger.warning(
                "An import error has been occurred while trying to import mlflow, "
                "make sure you have it installed in your python environment:\n%s",
                e,
            )
            return dbnd_store

        duplication_store = _get_store(duplicate_tracking_to, artifact_uri)

    try:
        from .databand_store import DatabandStore
    except ImportError as e:
        logger.warning(
            "An import error has been occurred while trying to import mlflow, "
            "make sure you have it installed in your python environment:\n%s",
            e,
        )
        return dbnd_store

    return DatabandStore(dbnd_store, duplication_store)
