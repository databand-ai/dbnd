# © Copyright Databand.ai, an IBM Company 2022

import pandas as pd

from dbnd_examples_orchestration.data import data_repo
from pandas import DataFrame

from dbnd import pipeline, task


class TestDocTasksPipelinesData:
    def test_pipelines(self):
        #### DOC START
        @task
        def prepare_data(data: pd.DataFrame) -> pd.DataFrame:
            return data

        @task
        def train_model(data: pd.DataFrame) -> object:
            ...

        @pipeline
        def prepare_data_pipeline(data: pd.DataFrame):
            prepared_data = prepare_data(data)
            model = train_model(prepared_data)
            return model

        #### DOC END
        prepare_data_pipeline.dbnd_run(data_repo.wines)

    def test_data(self):
        #### DOC START
        @task
        def prepare_data(data: DataFrame) -> DataFrame:
            data["new_column"] = 5
            return data

        #### DOC END
        prepare_data.dbnd_run(data=data_repo.wines)
