# Python 3.6.8
import logging

from typing import Tuple

from pandas import DataFrame, Series
from sklearn import datasets
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

from dbnd import log_dataframe, log_metric, pipeline, task


logging.basicConfig(level=logging.INFO)


@task(result="training_data, testing_data")
def prepare_data() -> Tuple[DataFrame, DataFrame]:
    """load dataset from sklearn. split into training and testing sets"""
    raw_data = datasets.load_diabetes()

    # create a pandas DataFrame from sklearn dataset
    df = DataFrame(raw_data["data"], columns=raw_data["feature_names"])
    df["target"] = Series(raw_data["target"])

    # split the data into training and testing sets
    training_data, testing_data = train_test_split(df, test_size=0.25)

    # use DBND logging features to log DataFrames with histograms
    log_dataframe(
        "training data",
        training_data,
        with_histograms=True,
        with_schema=True,
        with_stats=True,
    )
    log_dataframe("testing_data", testing_data)

    return training_data, testing_data


@task(result="model")
def train_model(training_data: DataFrame) -> LinearRegression:
    """train a linear regression model"""
    model = LinearRegression()

    # train a linear regression model
    model.fit(training_data.drop("target", axis=1), training_data["target"])

    # use DBND log crucial details about the regression model with log_metric:
    log_metric("model intercept", model.intercept_)  # logging a numeric
    log_metric("coefficients", model.coef_)  # logging an np array
    return model


@task(result="performance_metrics")
def test_model(model: LinearRegression, testing_data: DataFrame) -> str:
    """test the model, output mean squared error and r2 score"""
    testing_x = testing_data.drop("target", axis=1)
    testing_y = testing_data["target"]
    predictions = model.predict(testing_x)
    mse = mean_squared_error(testing_y, predictions)
    r2_score = model.score(testing_x, testing_y)

    # use DBND log_metric to capture important model details:
    log_metric("mean squared error:", mse)
    log_metric("r2 score", r2_score)

    return f"MSE: {mse}, R2: {r2_score}"


@pipeline(result=("model", "metrics"))
def linear_reg_pipeline():
    training_set, testing_set = prepare_data()
    model = train_model(training_set)
    metrics = test_model(model, testing_set)

    # return the model along with metrics
    return model, metrics
