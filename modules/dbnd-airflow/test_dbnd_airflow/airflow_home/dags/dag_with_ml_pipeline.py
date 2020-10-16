import logging
import time

from datetime import timedelta
from typing import Tuple

from airflow import DAG
from airflow.utils.dates import days_ago
from pandas import DataFrame

from dbnd import task


logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'dbnd_config': {
    #      "my_task.p_int": 4
    # }
}


def _split(data):
    half_len = int(len(data) / 2)
    return data.iloc[:, :half_len], data.iloc[:, half_len:]


@task(result=("training_set", "test_set", "validation_set", "good_alpha"))
def create_data_sets(data=None):
    # type: (DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame, bool]
    train_df, test_df = _split(data)
    test_df, validation_df = _split(test_df)

    return train_df, test_df, validation_df, True


@task
def calculate_alpha(alpha=0.5):
    # type: (float) -> float
    alpha -= 0.1
    return alpha


@task
def train_model(sets, alpha=0.5):
    # type: (Tuple[DataFrame, DataFrame], float) -> int
    logging.info("Training the model...")
    time.sleep(2)

    return 0


@task
def validate_model(model, validation_dataset):
    # type: (str, DataFrame) -> bool
    return not validation_dataset.is_copy and model is not None


@task
def get_data_frame(data=None):
    # type: (DataFrame) -> DataFrame
    logging.info("I found the data!")
    return data


alpha = 0.5
data_path = "../../../../examples/data/wine_quality.csv"

with DAG(dag_id="dbnd_split_data_frame", default_args=default_args) as dag_operators:
    data_frame = get_data_frame(data_path)
    train_set, test_set, validation_set, good_alpha = create_data_sets(data=data_frame)
    if good_alpha:
        alpha = calculate_alpha(alpha=alpha)

    model = train_model(sets=(train_set, test_set), alpha=alpha)

    validation = validate_model(model=model, validation_dataset=validation_set)

    logger.debug("Successfully read pipeline")

if __name__ == "__main__":
    dag_operators.clear()
    dag_operators.run()
