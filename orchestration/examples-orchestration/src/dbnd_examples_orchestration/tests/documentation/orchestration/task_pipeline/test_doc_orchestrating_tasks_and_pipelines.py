# © Copyright Databand.ai, an IBM Company 2022

from typing import Tuple

import pandas as pd

from dbnd_examples_orchestration.data import data_repo
from pandas import DataFrame
from sklearn.linear_model import ElasticNet
from sklearn.model_selection import train_test_split

from dbnd import pipeline, task


#### DOC START


# define a function with a decorator


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


@pipeline(result=("model", "validation_set"))
def predict_wine_quality(
    raw_data: DataFrame, alpha: float = 0.5, l1_ratio: float = 0.5
):
    training_set, validation_set = prepare_data(raw_data=raw_data)

    model = train_model(training_set=training_set, alpha=alpha, l1_ratio=l1_ratio)

    return model, validation_set


#### DOC END


class TestDocOrchestratingTasksAndPipelines:
    def test_doc(self):
        predict_wine_quality.task(raw_data=pd.read_csv(data_repo.wines)).dbnd_run()
