# © Copyright Databand.ai, an IBM Company 2022

import os
import tempfile

import pytest
import tensorflow as tf

from dbnd import output, pipeline, task
from dbnd._core.errors import DatabandRuntimeError
from dbnd_tensorflow.marshalling.tensorflow_marshaller import (
    TensorflowKerasHistoryMarshaller,
    TensorflowKerasModelMarshaller,
)
from targets import target


@pipeline
def model_pipe():
    model = model_builder()
    new_model = model_passer(model)
    return new_model


@pipeline
def train_pipe():
    model = model_builder()
    history = train_model(model)
    new_history = history_passer(history)
    return new_history


@task(result=output.tfmodel[tf.keras.models.Model])
def model_builder():
    """Builds simple CNN model for MNIST."""
    inputs = tf.keras.layers.Input((28, 28, 1), dtype=tf.float32, name="inputs")
    res = tf.keras.layers.Conv2D(
        filters=6, kernel_size=(5, 5), strides=(1, 1), activation=tf.nn.relu
    )(inputs)
    res = tf.keras.layers.Flatten()(res)
    return tf.keras.models.Model(inputs, res)


@task(result=output.tfmodel[tf.keras.models.Model])
def model_passer(model):
    print("Model passing by")
    print(model)
    return model


@task(result=output.tfhistory[tf.keras.callbacks.History])
def history_passer(history):
    print("History passing by")
    print(history)
    return history


@task(result=output.tfhistory[tf.keras.callbacks.History])
def train_model(model, learning_rate=0.001, epochs=1, batch_size=128):
    data = load_data()

    model.compile(
        optimizer=tf.keras.optimizers.SGD(learning_rate),
        loss="sparse_categorical_crossentropy",
        metrics=["accuracy"],
    )
    (x, y), validation_data = data
    history = model.fit(
        x, y, validation_data=validation_data, batch_size=batch_size, epochs=epochs
    )

    return history


def load_data():
    (x_train, y_train), (x_test, y_test) = tf.keras.datasets.mnist.load_data()
    x_train = x_train / 255.0
    x_test = x_test / 255.0
    x_train = x_train[..., None][:10000]
    x_test = x_test[..., None][:10000]
    y_train = y_train[..., None][:10000]
    y_test = y_test[..., None][:10000]
    return (x_train, y_train), (x_test, y_test)


class TestTensorflowMarshalling:
    @pytest.fixture(autouse=True)
    def model(self):
        return model_builder()

    @pytest.fixture(autouse=True)
    def temp_target(self):
        tempdir = tempfile.gettempdir()
        target_path = os.path.join(tempdir, "tf_object.tfmodel")
        yield target(target_path)
        os.system("rm -rf {0}".format(target_path))

    @pytest.fixture(autouse=True)
    def history(self, model):
        return train_model(model)

    def test_model_marshalling_sanity_run(self):
        model_builder.dbnd_run()

    def test_model_marshalling_flow_run(self):
        model_pipe.dbnd_run()

    def test_history_marshalling_flow_run(self):
        train_pipe.dbnd_run()

    def test_model_marshalling_value_to_target(self, model, temp_target):
        model_marshaller = TensorflowKerasModelMarshaller()
        model_marshaller.value_to_target(model, target=temp_target)
        assert os.path.exists(temp_target.path)

    def test_model_marshalling_target_to_value(self, model, temp_target):
        model_marshaller = TensorflowKerasModelMarshaller()
        tf.keras.models.save_model(model, temp_target.path)

        loaded_model = model_marshaller.target_to_value(target=temp_target)

        # Ensure all weights and variables are loaded correctly
        assert len(loaded_model.weights) == len(model.weights)
        assert len(loaded_model.variables) == len(model.variables)
        assert len(loaded_model.layers) == len(model.layers)

    def test_model_marshalling_target_to_value_remote_target(self, model):
        pytest.importorskip("dbnd_aws")
        model_marshaller = TensorflowKerasModelMarshaller()
        from dbnd_aws.fs import build_s3_fs_client
        from targets.fs import FileSystems, register_file_system

        register_file_system(FileSystems.s3, build_s3_fs_client)
        t = target("s3://path_to/my_bucket")
        with pytest.raises(DatabandRuntimeError):
            model_marshaller.target_to_value(t)

    def test_history_marshalling_value_to_target(self, history, temp_target):
        history_marshaller = TensorflowKerasHistoryMarshaller()
        history_marshaller.value_to_target(history, target=temp_target)
        assert os.path.exists(temp_target.path)

    def test_history_marshalling_target_to_value(self, history, temp_target):
        history_marshaller = TensorflowKerasHistoryMarshaller()
        history_marshaller.value_to_target(history, target=temp_target)
        loaded_history = history_marshaller.target_to_value(target=temp_target)

        # Compare the inner dictionary to ensure two objects are equal value-wise
        assert loaded_history.history == history.history
