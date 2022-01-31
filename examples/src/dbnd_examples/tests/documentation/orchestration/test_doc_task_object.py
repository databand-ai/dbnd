import pandas as pd

from dbnd import pipeline, task
from dbnd_examples.data import data_repo


@task
def prepare_data(alpha=0.5):
    return alpha


class TestDocTaskObject:
    def test_task_class_version(self):
        #### DOC START
        @task(task_class_version=2)
        def prepare_data(data: pd.DataFrame) -> pd.DataFrame:
            return data

        #### DOC END
        prepare_data.dbnd_run(data=data_repo.wines)

    def test_doc(self):
        #### DOC START
        c = prepare_data(alpha=0.5)
        d = prepare_data(alpha=0.5)

        assert c is d
        #### DOC END

    def test_prepare_data_pipeline(self):
        #### DOC START
        @pipeline
        def prepare_data_pipeline():
            prepare_data(task_name="first_prepare_data")
            prepare_data(task_name="second_prepare_data")

        #### DOC END
        prepare_data_pipeline.dbnd_run()

    def test_task_in_task(self):
        #### DOC START
        import logging

        from dbnd import task

        @task
        def calculate_alpha(alpha: int):
            calculated_alpha = "alpha: {}".format(alpha)
            logging.info(calculated_alpha)
            return calculated_alpha

        @task
        def prepare_data(data, additional_data):
            return "{} {}".format(data, additional_data)

        @task
        def prepare_dataset(data, data_num: int = 3) -> str:
            result = ""
            for i in range(data_num):
                result = prepare_data(result, data)

            return result

        #### DOCK END
        prepare_dataset.dbnd_run(data=data_repo.wines)
        calculate_alpha.dbnd_run(alpha=1)
