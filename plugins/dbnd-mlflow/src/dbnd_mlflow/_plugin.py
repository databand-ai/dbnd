import logging

from dbnd import config, hookimpl, register_config_cls
from dbnd._core.errors import DatabandConfigError

from .url_utils import build_composite_uri, is_composite_uri


logger = logging.getLogger(__name__)


@hookimpl
def dbnd_setup_plugin():
    from dbnd_mlflow.mlflow_config import MLFlowTrackingConfig

    register_config_cls(MLFlowTrackingConfig)


_original_mlflow_tracking_uri = None


@hookimpl
def dbnd_on_pre_init_context(ctx):
    from mlflow import get_tracking_uri, set_tracking_uri

    if not config.getboolean("mlflow_tracking", "databand_tracking"):
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


@hookimpl
def dbnd_on_exit_context(ctx):
    from mlflow import set_tracking_uri

    global _original_mlflow_tracking_uri
    set_tracking_uri(_original_mlflow_tracking_uri)
