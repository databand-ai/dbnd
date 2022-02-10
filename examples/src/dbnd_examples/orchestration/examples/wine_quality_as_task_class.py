# This example requires pandas, numpy, sklearn, scipy
# Inspired by an MLFlow tutorial:
#   https://github.com/databricks/mlflow/blob/master/example/tutorial/train.py


import itertools
import logging

import numpy as np

from pandas import DataFrame
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from dbnd import data, log_dataframe, log_metric, output, parameter
from dbnd.tasks import PipelineTask, PythonTask
from dbnd_examples.data import data_repo


logger = logging.getLogger(__name__)

test_data_csv = data_repo.wines


def _split(df):
    return df.head(int(len(df) / 2)), df.tail(int(len(df) / 2))


def calculate_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2


class PrepareData(PythonTask):
    """Split data into train, test and validation"""

    raw_data = parameter[DataFrame]

    training_set = output.csv
    test_set = output.csv
    validation_set = output.csv

    def run(self):
        self.log_dataframe("input", self.raw_data)

        train_df, test_df = train_test_split(self.raw_data)
        test_df, validation_df = train_test_split(test_df, test_size=0.5)

        self.log_dataframe("train_size", train_df)
        self.log_dataframe("test_size", test_df)
        self.log_dataframe("validation_df", validation_df)

        train_df.to_target(self.training_set, index=False)
        test_df.to_target(self.test_set, index=False)
        validation_df.to_target(self.validation_set, index=False)


class TrainModel(PythonTask):
    """Train wine prediction model"""

    alpha = parameter.value(0.5)
    l1_ratio = parameter.value(0.5)

    test_set = parameter[DataFrame]
    training_set = parameter[DataFrame]

    # fake parameter that represent quality
    quality = parameter.value(1.0)
    model = output.pickle.target

    def run(self):
        train = self.training_set
        test = self.test_set

        lr = ElasticNet(alpha=self.alpha, l1_ratio=self.l1_ratio)
        lr.fit(train.drop(["quality"], 1), train[["quality"]])
        prediction = lr.predict(test.drop(["quality"], 1))

        (rmse, mae, r2) = calculate_metrics(test[["quality"]], prediction)

        self.log_metric("train_size", len(train))
        self.log_metric("test_size", len(test))
        self.log_metric("rmse", rmse)
        self.log_metric("mae", rmse * self.quality)
        self.log_metric("r2", r2 * self.quality)

        self.model.write_pickle(lr)

        logging.info(
            "Elasticnet model (alpha=%f, l1_ratio=%f): rmse = %f, mae = %f, r2 = %f",
            self.alpha,
            self.l1_ratio,
            rmse,
            mae,
            r2,
        )


class ValidateModel(PythonTask):
    """Calculates metrics of wine prediction model"""

    model = parameter.data
    validation_dataset = parameter[DataFrame]
    model_metrics = output.csv.data

    def run(self):
        validation = self.validation_dataset
        self.log_metric("test_size", len(validation))

        actual_model = self.model.read_pickle()
        logger.info("%s", validation.shape)

        validation_x = validation.drop(["quality"], 1)
        validation_y = validation[["quality"]]

        prediction = actual_model.predict(validation_x)
        (rmse, mae, r2) = calculate_metrics(validation_y, prediction)

        log_dataframe("validation", validation)
        log_metric("rmse", rmse)
        log_metric("mae", rmse)
        log_metric("r2", r2)

        self.model_metrics.write("%s,%s,%s" % (rmse, mae, r2))


class PredictWineQuality(PipelineTask):
    """Entry point for wine quality prediction"""

    data = parameter(default=test_data_csv).data
    alpha = parameter.value(0.5)
    l1_ratio = parameter.value(0.5)

    model = output
    validation = output

    def band(self):
        datasets = PrepareData(raw_data=self.data)
        self.model = TrainModel(
            test_set=datasets.test_set,
            training_set=datasets.training_set,
            alpha=self.alpha,
            l1_ratio=self.l1_ratio,
        ).model

        self.validation = ValidateModel(
            model=self.model, validation_dataset=datasets.validation_set
        ).model_metrics


class PredictWineQualityParameterSearch(PipelineTask):
    data = data(default=test_data_csv).target
    alpha_step = parameter.value(0.3)
    l1_ratio_step = parameter.value(0.4)

    results = output

    def band(self):
        result = {}
        variants = list(
            itertools.product(
                np.arange(0, 1, self.alpha_step), np.arange(0, 1, self.l1_ratio_step)
            )
        )

        # variants = list(itertools.product([0.1, 0.5], [0.2, 0.3]))
        logger.info("All Variants: %s", variants)
        for alpha_value, l1_ratio in variants:
            predict = PredictWineQuality(
                data=self.data, alpha=alpha_value, l1_ratio=l1_ratio
            )

            exp_name = "%f_%f" % (alpha_value, l1_ratio)
            result[exp_name] = (predict.model, predict.validation)
        self.results = result
