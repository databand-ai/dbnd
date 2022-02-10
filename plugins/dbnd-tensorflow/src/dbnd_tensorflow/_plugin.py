import dbnd

from targets.marshalling import register_marshaller
from targets.target_config import FileFormat
from targets.values import register_value_type


@dbnd.hookimpl
def dbnd_setup_plugin():
    from tensorflow.keras import models
    from tensorflow.keras.callbacks import History

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
