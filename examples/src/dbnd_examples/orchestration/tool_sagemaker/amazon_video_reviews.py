# © Copyright Databand.ai, an IBM Company 2022

"""
Based on https://github.com/aws-samples/sagemaker-ml-workflow-with-apache-airflow example by Amazon.
distributed under permissive amazon licence: https://github.com/aws-samples/sagemaker-ml-workflow-with-apache-airflow/blob/master/LICENSE
"""
from __future__ import absolute_import

import io
import logging
import tempfile

from typing import Tuple

import numpy as np
import pandas as pd
import sagemaker.amazon.common as smac

from scipy.sparse import lil_matrix

from dbnd import log_dataframe, log_metric, output, pipeline, task
from dbnd_aws.sagemaker import SageMakerTrainTask
from dbnd_aws.sagemaker.estimator_config import Algorithm, GenericEstimatorConfig
from dbnd_examples.orchestration.tool_sagemaker import sagemaker_data_repo
from targets import Target
from targets.fs import FileSystems


logger = logging.getLogger(__name__)


@task(result="training_set, test_set, validation_set")
def preprocess(
    raw_data: pd.DataFrame = sagemaker_data_repo.amazon_reviews_raw_data,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Preprocesses data based on business logic
    - Reads delimited file passed as s3_url and preprocess data by filtering
    long tail in the customer ratings data i.e. keep customers who have rated 5
    or more videos, and videos that have been rated by 9+ customers
    - Preprocessed data is then written to output

    """
    # limit dataframe to customer_id, product_id, and star_rating
    # `product_title` will be useful validating recommendations
    df = raw_data[["customer_id", "product_id", "star_rating", "product_title"]]

    # clean out the long tail because most people haven't seen most videos,
    # and people rate fewer videos than they actually watch
    customers = df["customer_id"].value_counts()
    products = df["product_id"].value_counts()

    # based on data exploration only about 5% of customers have rated 5 or
    # more videos, and only 25% of videos have been rated by 9+ customers
    customers = customers[customers >= 5]
    products = products[products >= 10]
    log_dataframe("Original data shape", df)

    reduced_df = df.merge(pd.DataFrame({"customer_id": customers.index})).merge(
        pd.DataFrame({"product_id": products.index})
    )

    log_dataframe("Shape after removing long tail", reduced_df)

    reduced_df = reduced_df.drop_duplicates(["customer_id", "product_id"])
    log_dataframe("Shape after removing duplicates", reduced_df)

    # recreate customer and product lists since there are customers with
    # more than 5 reviews, but all of their reviews are on products with
    # less than 5 reviews (and vice versa)
    customers = reduced_df["customer_id"].value_counts()
    products = reduced_df["product_id"].value_counts()

    # sequentially index each user and item to hold the sparse format where
    # the indices indicate the row and column in our ratings matrix
    customer_index = pd.DataFrame(
        {"customer_id": customers.index, "customer": np.arange(customers.shape[0])}
    )
    product_index = pd.DataFrame(
        {"product_id": products.index, "product": np.arange(products.shape[0])}
    )
    reduced_df = reduced_df.merge(customer_index).merge(product_index)

    nb_customer = reduced_df["customer"].max() + 1
    nb_products = reduced_df["product"].max() + 1
    feature_dim = nb_customer + nb_products

    log_metric(
        "features(customer,product,total)", (nb_customer, nb_products, feature_dim)
    )
    # print(nb_customer, nb_products, feature_dim)

    product_df = reduced_df[["customer", "product", "star_rating"]]

    # split into train, validation and test data sets
    train_df, validate_df, test_df = np.split(
        product_df.sample(frac=1),
        [int(0.6 * len(product_df)), int(0.8 * len(product_df))],
    )

    log_metric("# of rows train", train_df.shape[0])
    log_metric("# of rows test", test_df.shape[0])
    log_metric("# of rows validation", validate_df.shape[0])

    # select columns required for training the model
    # excluding columns "customer_id", "product_id", "product_title" to
    # keep files small
    cols = ["customer", "product", "star_rating"]
    train_df = train_df[cols]
    validate_df = validate_df[cols]
    test_df = test_df[cols]

    return train_df, test_df, validate_df


def convert_sparse_matrix(df, nb_rows, nb_customer, nb_products):
    # dataframe to array
    df_val = df.values

    # determine feature size
    nb_cols = nb_customer + nb_products
    log_metric("# of rows", nb_rows)
    log_metric("# of cols", nb_cols)

    # extract customers and ratings
    df_X = df_val[:, 0:2]
    # Features are one-hot encoded in a sparse matrix
    X = lil_matrix((nb_rows, nb_cols)).astype("float32")
    df_X[:, 1] = nb_customer + df_X[:, 1]
    coords = df_X[:, 0:2]
    X[np.arange(nb_rows), coords[:, 0]] = 1
    X[np.arange(nb_rows), coords[:, 1]] = 1

    # create label with ratings
    Y = df_val[:, 2].astype("float32")

    # validate size and shape

    logger.info("Shape of X: %s ", str(X.shape))
    logger.info("Shape of Y: %s", str(Y.shape))
    assert X.shape == (nb_rows, nb_cols)
    assert Y.shape == (nb_rows,)

    return X, Y


def save_as_protobuf(X, Y, target):
    """Converts features and predictions matrices to recordio protobuf
    Args:
        X:
          2D numpy matrix with features
        Y:
          1D numpy matrix with predictions
        target:
          protobuf file name to be staged
    """
    buf = io.BytesIO()
    smac.write_spmatrix_to_sparse_tensor(buf, X, Y)

    f, name = tempfile.mkstemp()
    with open(f, "wb") as fd:
        fd.write(buf.getvalue())
        fd.seek(0)
        if target.fs.name == FileSystems.s3:
            target.fs.put(name, str(target))
        else:
            target.move_from(from_path=name)


def chunk(x, batch_size):
    """split array into chunks of batch_size"""
    chunk_range = range(0, x.shape[0], batch_size)
    chunks = [x[p : p + batch_size] for p in chunk_range]
    return chunks


@task
def prepare(
    train: pd.DataFrame,
    test: pd.DataFrame,
    validation: pd.DataFrame,
    train_out=output[Target],
    test_out=output.folder_data.with_flag(None)[Target],
    validation_out=output[Target],
):
    """Prepare data for training with Sagemaker algorithms

    - Read preprocessed data and converts to ProtoBuf format to prepare for
      training with Sagemaker algorithms

    Args:
        test - dataframe with test data
        train  - dataframe with train data
        validation - dataframe with validation data
        train_out - output path for train data in protobuf format
        test_out - output path for test data in protobuf format
        validation_out - output path for validation data in protobuf format
    Returns:
        s3 url with key to the prepared data
    """
    all_df = pd.concat([train, validation, test])
    nb_customer = np.unique(all_df["customer"].values).shape[0]
    nb_products = np.unique(all_df["product"].values).shape[0]
    feature_dim = nb_customer + nb_products
    log_metric(
        "customers x products x feature_dim", (nb_customer, nb_products, feature_dim)
    )

    train_X, train_Y = convert_sparse_matrix(
        train, train.shape[0], nb_customer, nb_products
    )
    validate_X, validate_Y = convert_sparse_matrix(
        validation, validation.shape[0], nb_customer, nb_products
    )
    test_X, test_Y = convert_sparse_matrix(
        test, test.shape[0], nb_customer, nb_products
    )

    save_as_protobuf(train_X, train_Y, train_out)
    save_as_protobuf(validate_X, validate_Y, validation_out)

    test_x_chunks = chunk(test_X, 10000)
    test_y_chunks = chunk(test_Y, 10000)
    N = len(test_x_chunks)
    for i in range(N):
        save_as_protobuf(test_x_chunks[i], test_y_chunks[i], test_out.partition())
    return "OK"


@pipeline
def train_review_model(
    raw_data: pd.DataFrame = sagemaker_data_repo.amazon_reviews_raw_data,
):
    train, test, validate = preprocess(raw_data=raw_data)
    prepared = prepare(train=train, test=test, validation=validate)

    hyperparameters = {
        "feature_dim": "178729",
        "epochs": "10",
        "mini_batch_size": "200",
        "num_factors": "64",
        "predictor_type": "regressor",
    }

    train = SageMakerTrainTask(
        train=prepared.train_out,
        estimator_config=GenericEstimatorConfig(
            algorithm=Algorithm.FACTORIZATION_MACHINES, hyperparameters=hyperparameters
        ),
    )

    return prepared, train.output_path
