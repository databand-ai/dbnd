import logging

import dbnd

from targets.marshalling import register_marshaller
from targets.target_config import FileFormat
from targets.values import register_value_type


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_setup_plugin():
    try:
        from tensorflow.keras import models
        from tensorflow.keras.callbacks import History
    except ImportError:
        logger.warning(
            "An import error has been occurred while trying to import tensorflow, "
            "make sure you have it installed in your python environment."
        )
        return

    from dbnd_tensorflow.marshalling.tensorflow_marshaller import (
        TensorflowKerasHistoryMarshaller,
        TensorflowKerasModelMarshaller,
    )
    from dbnd_tensorflow.marshalling.tensorflow_values import (
        TensorflowHistoryValueType,
        TensorflowModelValueType,
    )

    register_marshaller(
        models.Model, FileFormat.tfmodel, TensorflowKerasModelMarshaller()
    )
    register_marshaller(
        History, FileFormat.tfhistory, TensorflowKerasHistoryMarshaller()
    )

    register_value_type(TensorflowModelValueType())
    register_value_type(TensorflowHistoryValueType())
