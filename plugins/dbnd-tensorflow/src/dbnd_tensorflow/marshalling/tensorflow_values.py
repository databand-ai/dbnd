import tensorflow

from dbnd_tensorflow.seven import History, models
from targets.values.builtins_values import DataValueType


class TensorflowModelValueType(DataValueType):
    type = models.Model
    type_str = "Model"
    config_name = "tensorflow_model"


class TensorflowHistoryValueType(DataValueType):
    type = History
    type_str = "History"
    config_name = "tensorflow_history"
