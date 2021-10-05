# https://docs.databand.ai/docs/running-pipelines

import pandas as pd

import databand

from dbnd import PipelineTask, output, parameter, pipeline, task
from dbnd_examples.data import data_repo


#### DOC START
@task
def prepare_data(data: pd.DataFrame) -> pd.DataFrame:
    return data


#### DOC END


class TestDocTasksPipelinesData(object):
    def test_prepare_data_decorated_function(self):
        prepare_data(data=data_repo.wines)

    def test_prepare_data_derived_class(self):
        #### DOC START
        class PrepareData(databand.Task):
            data = parameter.data
            prepared_data = output.csv.data

            def run(self):
                self.prepared_data.write(self.data)

        #### DOC END
        PrepareData(data=data_repo.wines).dbnd_run()

    def test_prepare_data_decorated_pipeline(self):
        #### DOC START
        @pipeline(result=("prepared_data", "raw_data"))
        def prepare_data_pipeline(data: pd.DataFrame):
            return prepare_data(data), data

        #### DOC END
        prepare_data_pipeline.dbnd_run(data=data_repo.wines)

    def test_prepare_data_pipeline_class(self):
        #### DOC START
        class PrepareData(PipelineTask):
            data = parameter.data

            prepared_data = output.csv.data

            def band(self):
                self.prepared_data = prepare_data(data=self.data)

        #### DOC END
        PrepareData(data=data_repo.wines).dbnd_run()
