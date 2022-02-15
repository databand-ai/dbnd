from pandas import DataFrame

from dbnd import pipeline, task
from dbnd_examples.data import data_repo


@task
def prepare_data(raw_data: DataFrame = data_repo.wines):
    return raw_data


@task
def calculate_alpha(alpha: float = 0.5):
    return alpha


@task
def calculate_beta(beta: float = 0.1):
    return beta


@task
def calculate_coefficient(alpha, beta):
    return alpha / beta


class TestDocTasksWiringAtPipeline:
    def test_pipeline(self):
        ####DOC START
        @pipeline
        def evaluate_data(raw_data: DataFrame):
            data = prepare_data(raw_data=raw_data)
            alpha = calculate_alpha()
            beta = calculate_beta()
            coefficient = calculate_coefficient(alpha, beta)

            return data, coefficient

        @task
        def train_model(data: DataFrame, coefficient):
            model = data, coefficient
            return model

        @pipeline
        def create_model(raw_data: DataFrame):
            data, coefficient = evaluate_data(raw_data)
            return train_model(data, coefficient)

        #### DOC END

        create_model.dbnd_run(raw_data=data_repo.wines)

    def test_linear_reg_pipeline(self):
        @task
        def prepare_data() -> (str, str):
            return "training_set", "testing_set"

        @task
        def train_model(training_set):
            return "model"

        @task
        def test_model(model, testing_set):
            return "metrics"

        @task
        def get_validation_metrics(testing_set):
            return "validation_metrics"

        #### DOC START
        @pipeline(result=("model", "metrics", "validation_metrics"))
        def linear_reg_pipeline():
            training_set, testing_set = prepare_data()
            model = train_model(training_set)
            metrics = test_model(model, testing_set)
            validation_metrics = get_validation_metrics(testing_set)
            validation_metrics.task.set_upstream(metrics.task)
            # return the model along with metrics
            return model, metrics, validation_metrics

        #### DOC END

        linear_reg_pipeline.task().dbnd_run()
