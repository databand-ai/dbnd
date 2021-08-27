#### TESTED

from typing import Tuple

import pandas as pd

from pandas import DataFrame

from dbnd import parameter, task
from dbnd_examples.data import data_repo
from targets.target_config import FileFormat


class TestDocDataSerializationDeSerialization:
    def test_prepare_data_save_result_with_no_header(self):
        #### DOC START
        @task(result=parameter.csv.save_options(FileFormat.csv, header=False))
        def prepare_data(data: DataFrame) -> DataFrame:
            data["new_column"] = 5
            return data

        #### DOC END
        prepare_data.task(data=data_repo.wines).dbnd_run()

    def test_prepare_data_tab_delimited(self):
        #### DOC START
        @task(data=parameter[DataFrame].csv.load_options(FileFormat.csv, sep="\t"))
        def prepare_data(data: DataFrame) -> DataFrame:
            data["new_column"] = 5
            return data

        #### DOC END
        prepare_data.task(data=data_repo.wines).dbnd_run()

    def test_prepare_data(self):
        #### DOC START
        @task
        def prepare_data(data: DataFrame) -> DataFrame:
            data["new_column"] = 5
            return data

        #### DOC END
        prepare_data.task(data=data_repo.wines).dbnd_run()

    def test_tuple_two_outputs(self):
        #### DOC START
        @task(result="training_set,real_data")
        def prepare_data(data: DataFrame) -> Tuple[DataFrame, DataFrame]:
            data["new_column"] = 5
            return data, data

        #### DOC END

        prepare_data.task(data=data_repo.wines).dbnd_run()
