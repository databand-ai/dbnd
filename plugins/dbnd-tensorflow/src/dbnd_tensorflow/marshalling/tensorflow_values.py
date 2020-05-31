import tensorflow

from targets.values.builtins_values import DataValueType


class TensorflowModelValueType(DataValueType):
    type = tensorflow.keras.models.Model
    type_str = "Model"
    config_name = "tensorflow_model"


class TensorflowHistoryValueType(DataValueType):
    type = tensorflow.python.keras.callbacks.History
    type_str = "History"
    config_name = "tensorflow_history"
