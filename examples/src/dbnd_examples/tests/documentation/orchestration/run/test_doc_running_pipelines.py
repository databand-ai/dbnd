#### TESTED

#### DOC START
from dbnd import PythonTask, parameter, pipeline, task


@task
def prepare_data(data: str) -> str:
    return data


@pipeline(result=("clone1", "clone2"))
def prepare_data_pipeline(data1: str, data2: str) -> (str, str):
    return (prepare_data(data=data1)), prepare_data(data=data2)


#### DOC END


class TestDocRunningPipelines:
    def test_doc(self):
        prepare_data_pipeline.task(data1="hello", data2="goodbye").dbnd_run()
