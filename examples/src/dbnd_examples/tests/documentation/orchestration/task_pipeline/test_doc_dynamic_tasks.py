#### TESTED
import logging

from dbnd import task


class TestDocDynamicTasks:
    def test_prepare_data(self):
        #### DOC START
        @task
        def calculate_alpha(alpha: int):
            calculated_alpha = "alpha: {}".format(alpha)
            logging.info(calculated_alpha)
            return calculated_alpha

        #### DOC END
        calculate_alpha.task(alpha=0.5).dbnd_run()

    def test_prepare_dataset(self):
        #### DOC START
        @task
        def prepare_data(data, additional_data):
            return "{} {}".format(data, additional_data)

        @task
        def prepare_dataset(data, data_num: int = 3) -> str:
            result = ""
            for i in range(data_num):
                result = prepare_data(result, data)

            return result

        #### DOC END
        prepare_dataset.task(data="Hello! ").dbnd_run()
