"""from dbnd import config, output, parameter
from dbnd_spark import spark, spark_task
from dbnd_spark.local.local_spark_config import SparkLocalEngineConfig

def assert_run_task(task):  # type: (Union[T, Task]) -> Union[T, Task]
    task.dbnd_run()
    assert task._complete()
    return task

#### DOC START


@spark_task(result=output[spark.DataFrame])
def word_count_inline(data=parameter.csv[spark.DataFrame]):
    # spark business logic goes here
    # set a breakpoint here with a debuger of your choice
    ...


# invoke spark task  this way
if __name__ == "__main__":
    # create spark context and run spark task inside this context
    with spark.SparkSession.builder.getOrCreate() as sc:
        word_count_inline.dbnd_run(text=__file__)


def test_spark_inline_same_context(self):
    from pyspark.sql import SparkSession
    from dbnd_examples.orchestration.dbnd_spark.word_count import word_count_inline

    with SparkSession.builder.getOrCreate() as sc:
        with config({SparkLocalEngineConfig.enable_spark_context_inplace: True}):
            task_instance = word_count_inline.t(text=__file__)
            assert_run_task(task_instance)


#### DOC END"""
