#### DOC START
from dbnd import PipelineTask, PythonTask, dbnd_run_cmd, parameter, task


class PrepareData(PythonTask):
    data = parameter(default="default")[str]

    def run(self):
        print("this is my data", self.data)


class PrepareDataPipeline(PipelineTask):
    def band(self):
        PrepareData(task_name="first_PrepareData").dbnd_run()
        PrepareData(task_name="second_PrepareData").dbnd_run()


#### DOC END


class TestDocConfiguringMultiInstanceTasks:
    def test_doc(self):
        PrepareDataPipeline().band()
