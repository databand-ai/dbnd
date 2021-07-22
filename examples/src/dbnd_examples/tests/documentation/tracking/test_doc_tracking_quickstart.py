#### DOC START
# Python 3.6.8
from typing import Tuple

from pandas import DataFrame, Series
from sklearn import datasets
from sklearn.model_selection import train_test_split


def prepare_data() -> Tuple[DataFrame, DataFrame]:
    """ load dataset from sklearn. split into training and testing sets"""
    raw_data = datasets.load_diabetes()

    # create a pandas DataFrame from sklearn dataset
    df = DataFrame(raw_data["data"], columns=raw_data["feature_names"])
    df["target"] = Series(raw_data["target"])

    # split the data into training and testing sets
    training_data, testing_data = train_test_split(df, test_size=0.25)

    return training_data, testing_data
