# © Copyright Databand.ai, an IBM Company 2022

from dbnd_examples_orchestration.data import data_repo

from dbnd import PipelineTask, Task, output, parameter


def do_something_with_data(data):
    return data


class TestDocClassDefinitionsAsAClass:
    def test_doc(self):
        #### DOC START
        class PrepareData(Task):
            data = parameter.data

            prepared_data = output.csv.data

            def run(self):
                self.prepared_data = do_something_with_data(self.data)

        class PrepareDataPipeline(PipelineTask):
            data = parameter.data

            prepared_data = output.csv.data

            def band(self):
                self.prepared_data = PrepareData(data=self.data).prepared_data

        #### DOC START

        PrepareDataPipeline(data=data_repo.wines).band()

    def test_task_decorator(self):
        #### DOC START
        from dbnd import task

        @task
        class PrepareData:
            data = parameter.data

            prepared_data = output.csv.data

            def run(self):
                self.prepared_data = do_something_with_data(self.data)

        #### DOC START
