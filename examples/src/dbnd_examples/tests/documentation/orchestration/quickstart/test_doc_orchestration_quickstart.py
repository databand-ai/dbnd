import logging

from typing import Tuple

import numpy as np
import pandas as pd

from pandas import DataFrame
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from dbnd import log_dataframe, log_metric, pipeline, task
from dbnd_examples.data import data_repo


def train_test_split(raw_data):
    return raw_data, raw_data


class TestDocOrchestrationQuickstart:
    def test_calculate_alpha(self):
        #### DOC START
        from dbnd import task

        @task
        def calculate_alpha(alpha: float = 0.5) -> float:
            alpha += 0.1
            return alpha

        #### DOC END
        calculate_alpha.dbnd_run(alpha=0.4)

    def test_without_dbnd(self):
        #### DOC START
        logging.basicConfig(level=logging.INFO)

        def training_script():
            # load data
            raw_data = pd.read_csv(data_repo.wines)

            # split data into training and validation sets
            train_df, validation_df = train_test_split(raw_data)

            # create hyperparameters and model
            alpha = 0.5
            l1_ratio = 0.2
            lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
            lr.fit(train_df.drop(["quality"], 1), train_df[["quality"]])

            # validation
            validation_x = validation_df.drop(["quality"], 1)
            validation_y = validation_df[["quality"]]
            prediction = lr.predict(validation_x)
            rmse = np.sqrt(mean_squared_error(validation_y, prediction))
            mae = mean_absolute_error(validation_y, prediction)
            r2 = r2_score(validation_y, prediction)

            logging.info("%s,%s,%s", rmse, mae, r2)

            return lr

        training_script()
        #### DOC END

    def test_using_dbnd(self):
        logging.basicConfig(level=logging.INFO)

        @task(result="training_set, validation_set")
        def prepare_data(raw_data: DataFrame) -> Tuple[DataFrame, DataFrame]:
            train_df, validation_df = train_test_split(raw_data)

            return train_df, validation_df

        @task
        def train_model(
            training_set: DataFrame, alpha: float = 0.5, l1_ratio: float = 0.5
        ) -> ElasticNet:
            lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
            lr.fit(training_set.drop(["quality"], 1), training_set[["quality"]])

            return lr

        @task
        def validate_model(model: ElasticNet, validation_dataset: DataFrame) -> str:
            log_dataframe("validation", validation_dataset)

            validation_x = validation_dataset.drop(["quality"], 1)
            validation_y = validation_dataset[["quality"]]

            prediction = model.predict(validation_x)
            rmse = np.sqrt(mean_squared_error(validation_y, prediction))
            mae = mean_absolute_error(validation_y, prediction)
            r2 = r2_score(validation_y, prediction)

            log_metric("rmse", rmse)
            log_metric("mae", rmse)
            log_metric("r2", r2)

            return "%s,%s,%s" % (rmse, mae, r2)

        @pipeline(result=("model", "validation"))
        def predict_wine_quality(
            raw_data: DataFrame, alpha: float = 0.5, l1_ratio: float = 0.5
        ):
            training_set, validation_set = prepare_data(raw_data=raw_data)

            model = train_model(
                training_set=training_set, alpha=alpha, l1_ratio=l1_ratio
            )
            validation = validate_model(model=model, validation_dataset=validation_set)

            return model, validation

        #### DOC END
        predict_wine_quality.dbnd_run(raw_data=data_repo.wines)
