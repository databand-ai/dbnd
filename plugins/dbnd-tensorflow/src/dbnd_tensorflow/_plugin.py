import dbnd
import tensorflow

from dbnd_tensorflow.marshalling.tensorflow_marshaller import (
    TensorflowKerasHistoryMarshaller,
    TensorflowKerasModelMarshaller,
)
from dbnd_tensorflow.marshalling.tensorflow_values import (
    TensorflowHistoryValueType,
    TensorflowModelValueType,
)
from targets.marshalling import register_marshaller
from targets.target_config import FileFormat
from targets.values import register_value_type


@dbnd.hookimpl
def dbnd_setup_plugin():
    register_marshaller(
        tensorflow.python.keras.engine.training.Model,
        FileFormat.tfmodel,
        TensorflowKerasModelMarshaller(),
    )
    register_marshaller(
        tensorflow.python.keras.callbacks.History,
        FileFormat.tfhistory,
        TensorflowKerasHistoryMarshaller(),
    )

    register_value_type(TensorflowModelValueType())
    register_value_type(TensorflowHistoryValueType())
