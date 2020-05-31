import tensorflow as tf

from dbnd import dbnd_config, pipeline, task
from dbnd._core.settings import RunConfig
from dbnd._vendor.cloudpickle import cloudpickle


@pipeline
def tf_pipeline():
    tf_func()
    tf_func()


@task
def tf_func():
    tf.keras.Sequential


class TestTensorflowSerialization:
    def test_serialization_simple(self):
        # Raises max recursion error in native CloudPickle
        cloudpickle.loads(cloudpickle.dumps(tf_func))

    def test_serialization_runtime(self):
        # Serialization in runtime is achieved by running in parallel
        with dbnd_config(
            {RunConfig.parallel: True, RunConfig.enable_concurent_sqlite: True}
        ):
            tf_pipeline.dbnd_run()
