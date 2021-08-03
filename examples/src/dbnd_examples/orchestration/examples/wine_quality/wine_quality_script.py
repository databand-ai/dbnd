import logging

import numpy as np
import pandas as pd

from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from dbnd_examples.data import data_repo


logging.basicConfig(level=logging.INFO)


def my_training_script():
    # load data
    raw_data = pd.read_csv(data_repo.wines_full)

    # Originally in the documentation we have this line instead:
    # raw_data = pd.read_csv("wine.csv")

    # split data into training and validation sets
    train_df, validation_df = train_test_split(raw_data, test_size=0.5)

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
