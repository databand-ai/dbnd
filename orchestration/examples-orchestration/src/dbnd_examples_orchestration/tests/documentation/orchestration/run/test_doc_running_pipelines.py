# © Copyright Databand.ai, an IBM Company 2022

from dbnd import task


class TestDocRunningPipelines:
    def test_doc(self):
        #### DOC START

        @task
        def prepare_data(data: str) -> str:
            return data

        #### DOC END
        prepare_data.dbnd_run(data="Hello Databand!")
