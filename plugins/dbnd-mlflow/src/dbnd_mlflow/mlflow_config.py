# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd import parameter
from dbnd._core.task.config import Config


logger = logging.getLogger(__name__)


class MLFlowTrackingConfig(Config):
    """MLFlow tracking config"""

    _conf__task_family = "mlflow_tracking"

    databand_tracking = parameter(
        default=False,
        description="Activate tracking of MLFlow metrics to databand store",
    )[bool]

    duplicate_tracking_to = parameter(
        default=None, description="By default mlflow.get_tracking_uri() will be used"
    )[str]
