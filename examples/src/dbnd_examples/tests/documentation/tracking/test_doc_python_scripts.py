# © Copyright Databand.ai, an IBM Company 2022

####rewrote

from pandas import DataFrame

from dbnd_examples.data import data_repo


class TestDocPythonScripts:
    def test_doc(self):
        #### DOC START
        import logging

        from typing import Tuple

        from dbnd import dbnd_tracking, task

        @task
        def prepare_data(data: DataFrame = data_repo.wines):
            prepared_data = data
            logging.info(prepared_data)
            return data

        @task
        def join_data(base_data: DataFrame, extra_data: DataFrame):
            return base_data.join(extra_data)

        @task
        def join_data_pipeline(dataframes_number=3):
            data = prepare_data(data_repo.wines)
            for i in range(dataframes_number):
                data = join_data(data, prepare_data(data_repo.wines))
            return data

        @task
        def prepare_all_data(dataframes_number=3) -> Tuple[DataFrame, DataFrame]:
            data = DataFrame.empty
            for i in range(dataframes_number):
                data = prepare_data(data_repo.wines)

            join_data_pipe = join_data_pipeline()
            return data, join_data_pipe

        # if __name__ == "__main__":
        # os.environ["DBND__CORE__DATABAND_URL"] = "<url>"
        # os.environ["DBND__CORE__DATABAND_ACCESS_TOKEN"] = "<access_token>"
        with dbnd_tracking():
            prepare_all_data()
        #### DOC END
