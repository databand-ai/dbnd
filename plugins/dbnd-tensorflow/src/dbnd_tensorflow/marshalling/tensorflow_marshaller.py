import tensorflow

from dbnd._core.errors import friendly_error
from dbnd._vendor.cloudpickle import cloudpickle
from targets import LocalFileSystem
from targets.marshalling.marshaller import Marshaller


class TensorflowKerasModelMarshaller(Marshaller):
    type = tensorflow.keras.models.Model
    support_directory_read = False

    _compression_read_arg = None
    _compression_write_arg = None

    support_cache = False
    disable_default_index = False

    def target_to_value(self, target, **kwargs):
        if not isinstance(target.fs, LocalFileSystem):
            raise friendly_error.targets.target_must_be_local_for_tensorflow_marshalling(
                target
            )
        model = tensorflow.keras.models.load_model(target.path, **kwargs)
        return model

    def value_to_target(self, value, target, **kwargs):
        tensorflow.keras.models.save_model(value, target.path, **kwargs)


class TensorflowKerasHistoryMarshaller(Marshaller):
    type = tensorflow.python.keras.callbacks.History

    def target_to_value(self, target, **kwargs):
        h = tensorflow.python.keras.callbacks.History()
        with target.open("rb") as dumped:
            h.history = cloudpickle.load(dumped)
        return h

    def value_to_target(self, value, target, **kwargs):
        with target.open("wb") as dumped:
            cloudpickle.dump(obj=value.history, file=dumped)
