# © Copyright Databand.ai, an IBM Company 2022

from pathlib import Path
from typing import NamedTuple, Tuple

import pandas as pd

from pandas import DataFrame

from dbnd import output, parameter, task
from dbnd_examples.data import data_repo
from targets.target_config import FileFormat


DEFAULT_OUTPUT_HDFS_PATH = data_repo.wines


class TestDocOutputs:
    def test_prepare_data_named_output(self):
        #### DOC START
        @task(result="data")
        def prepare_data(data: DataFrame):
            return data

        #### DOC END
        prepare_data.dbnd_run(data=data_repo.wines)

    def test_prepare_data_txt_output(self):
        #### DOC START
        @task(result=output.txt)
        def prepare_data(data: DataFrame):
            return data

        #### DOC END

        prepare_data.dbnd_run(data=data_repo.wines)

    def test_prepare_data_defined_path(self):
        #### DOC START
        @task(custom_output=output[Path])
        def prepare_data(data_path: Path, custom_output: Path) -> str:
            with open(str(custom_output), "w") as fp:
                fp.write("my data successfully written")
            return str(data_path)

        #### DOC END
        # prepare_data.dbnd_run(
        #    data_path=data_repo.wines, custom_output="~/data/example.txt"
        # )

    def test_prepare_data_overwrite_previous_result(self):
        #### DOC START
        @task(
            result=output(default=DEFAULT_OUTPUT_HDFS_PATH).overwrite.csv[pd.DataFrame]
        )
        def prepare_data(data=parameter[pd.DataFrame]):
            return data

        #### DOC END
        prepare_data.dbnd_run(data=data_repo.wines)

    def test_prepare_data_tuples(self):
        #### DOC START
        @task(result="training_set,real_data")
        def prepare_data(data: DataFrame) -> Tuple[DataFrame, DataFrame]:
            data["new_column"] = 5
            return data, data

        #### DOC END
        prepare_data.dbnd_run(data_repo.wines)

    def test_prepare_data_two_outputs(self):
        #### DOC START
        @task(result=("training_set", "real_data"))
        def prepare_data(p: int = 1) -> (DataFrame, DataFrame):
            return (
                pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
                pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
            )

        #### DOC END
        prepare_data.dbnd_run()

    def test_prepare_data_no_output_names(self):
        #### DOC START
        @task
        def prepare_data(p: int) -> (DataFrame, DataFrame):
            return (
                pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
                pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
            )

        #### DOC END
        prepare_data.dbnd_run(p=1)

    def test_prepare_data_with_tuple(self):
        #### DOC START
        Outputs = NamedTuple("outputs", training_set=DataFrame, real_data=DataFrame)

        @task
        def prepare_data(p: int) -> Outputs:
            return Outputs(
                pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
                pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
            )

        #### DOC END
        prepare_data.dbnd_run(p=1)

    def test_prepare_data_with_tuple_inside_task(self):
        #### DOC START
        @task
        def prepare_data(
            p: int,
        ) -> NamedTuple(
            "Outputs", fields=[("training_set", DataFrame), ("real_data", DataFrame)]
        ):
            return (
                pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
                pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
            )

        #### DOC END
        prepare_data.dbnd_run(p=1)

    def test_no_tuple_multiple_outputs(self):
        #### DOC START
        @task(result="training_set,real_data")
        def prepare_data(p: int = 1) -> (DataFrame, DataFrame):
            return (
                pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
                pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]),
            )

        #### DOC END
        prepare_data.task(p=3).dbnd_run()

    def test_prepare_data_no_header(self):
        #### DOC START
        @task(result=parameter.csv.save_options(FileFormat.csv, header=False))
        def prepare_data(data: DataFrame) -> DataFrame:
            data["new_column"] = 5
            return data

        #### DOC END
        prepare_data.dbnd_run(data_repo.wines)
