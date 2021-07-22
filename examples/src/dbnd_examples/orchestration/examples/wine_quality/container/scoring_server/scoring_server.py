"""
Scoring server for python model format.
The passed int model is expected to have function:
   predict(pandas.Dataframe) -> pandas.DataFrame

Input, expected intext/csv or application/json format,
is parsed into pandas.DataFrame and passed to the model.

Defines two endpoints:
    /ping used for health check
    /invocations used for scoring
"""


from __future__ import print_function

import json
import logging
import traceback

from json import JSONEncoder

import flask
import numpy as np
import pandas as pd

from six import reraise


# curl -X POST -H "Content-Type:application/json; format=pandas-split" --data '{"columns":["alcohol", "chlorides", "citric acid", "density", "fixed acidity", "free sulfur dioxide", "pH", "residual sugar", "sulphates", "total sulfur dioxide", "volatile acidity"],"data":[[12.8, 0.029, 0.48, 0.98, 6.2, 29, 3.33, 1.2, 0.39, 75, 0.66]]}' http://127.0.0.1:1234/invocations

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

CONTENT_TYPE_CSV = "text/csv"
CONTENT_TYPE_JSON = "application/json"
CONTENT_TYPE_JSON_RECORDS_ORIENTED = "application/json; format=pandas-records"
CONTENT_TYPE_JSON_SPLIT_ORIENTED = "application/json; format=pandas-split"

CONTENT_TYPES = [
    CONTENT_TYPE_CSV,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_JSON_RECORDS_ORIENTED,
    CONTENT_TYPE_JSON_SPLIT_ORIENTED,
]


_logger = logging.getLogger(__name__)


def parse_json_input(json_input, orientation="split"):
    """
    :param json_input: A JSON-formatted string representation of a Pandas DataFrame, or a stream
                       containing such a string representation.
    :param orientation: The Pandas DataFrame orientation of the JSON input. This is either 'split'
                        or 'records'.
    """
    # pylint: disable=broad-except
    try:
        return pd.read_json(json_input, orient=orientation)
    except Exception:
        _handle_serving_error(
            error_message=(
                "Failed to parse input as a Pandas DataFrame. Ensure that the input is"
                " a valid JSON-formatted Pandas DataFrame with the `{orientation}` orientation"
                " produced using the `pandas.DataFrame.to_json(..., orient='{orientation}')`"
                " method.".format(orientation=orientation)
            )
        )


def parse_csv_input(csv_input):
    """
    :param csv_input: A CSV-formatted string representation of a Pandas DataFrame, or a stream
                      containing such a string representation.
    """
    # pylint: disable=broad-except
    try:
        return pd.read_csv(csv_input)
    except Exception:
        _handle_serving_error(
            error_message=(
                "Failed to parse input as a Pandas DataFrame. Ensure that the input is"
                " a valid CSV-formatted Pandas DataFrame produced using the"
                " `pandas.DataFrame.to_csv()` method."
            )
        )


def _handle_serving_error(error_message):
    """
    Logs information about an exception thrown by model inference code that is currently being
    handled and reraises it with the specified error message. The exception stack trace
    is also included in the reraised error message.

    :param error_message: A message for the reraised exception.
    :param error_code: An appropriate error code for the reraised exception. This should be one of
                       the codes listed in the `mlflow.protos.databricks_pb2` proto.
    """
    traceback_buf = StringIO()
    traceback.print_exc(file=traceback_buf)
    reraise(
        Exception,
        Exception(message=error_message, stack_trace=traceback_buf.getvalue()),
    )


logged_pandas_records_format_warning = False


def init(model):
    """
    Initialize the server. Loads pyfunc model from the path.
    """
    app = flask.Flask(__name__)

    @app.route("/ping", methods=["GET"])
    def ping():  # pylint: disable=unused-variable
        """
        Determine if the container is working and healthy.
        We declare it healthy if we can load the model successfully.
        """
        health = model is not None
        status = 200 if health else 404
        return flask.Response(response="\n", status=status, mimetype="application/json")

    @app.route("/invocations", methods=["POST"])
    def transformation():  # pylint: disable=unused-variable
        """
        Do an inference on a single batch of data. In this sample server,
        we take data as CSV or json, convert it to a Pandas DataFrame,
        generate predictions and convert them back to CSV.
        """
        # Convert from CSV to pandas
        if flask.request.content_type == CONTENT_TYPE_CSV:
            data = flask.request.data.decode("utf-8")
            csv_input = StringIO(data)
            data = parse_csv_input(csv_input=csv_input)
        elif flask.request.content_type == CONTENT_TYPE_JSON:
            global logged_pandas_records_format_warning
            if not logged_pandas_records_format_warning:
                _logger.warning(
                    "**IMPORTANT UPDATE**: Starting in MLflow 0.9.0, requests received with a"
                    " `Content-Type` header value of `%s` will be interpreted"
                    " as JSON-serialized Pandas DataFrames with the `split` orientation, instead"
                    " of the `records` orientation. The `records` orientation is unsafe because"
                    " it may not preserve column ordering. Client code should be updated to"
                    " either send serialized DataFrames with the `split` orientation and the"
                    " `%s` content type (recommended) or use the `%s` content type with the"
                    " `records` orientation. For more information, see"
                    " https://www.mlflow.org/docs/latest/models.html#pyfunc-deployment.\n",
                    CONTENT_TYPE_JSON,
                    CONTENT_TYPE_JSON_SPLIT_ORIENTED,
                    CONTENT_TYPE_JSON_RECORDS_ORIENTED,
                )
                logged_pandas_records_format_warning = True
            data = parse_json_input(
                json_input=flask.request.data.decode("utf-8"), orientation="records"
            )
        elif flask.request.content_type == CONTENT_TYPE_JSON_RECORDS_ORIENTED:
            data = parse_json_input(
                json_input=flask.request.data.decode("utf-8"), orientation="records"
            )
        elif flask.request.content_type == CONTENT_TYPE_JSON_SPLIT_ORIENTED:
            data = parse_json_input(
                json_input=flask.request.data.decode("utf-8"), orientation="split"
            )
        else:
            return flask.Response(
                response=(
                    "This predictor only supports the following content types,"
                    " {supported_content_types}. Got '{received_content_type}'.".format(
                        supported_content_types=CONTENT_TYPES,
                        received_content_type=flask.request.content_type,
                    )
                ),
                status=415,
                mimetype="text/plain",
            )

        # Do the prediction
        # pylint: disable=broad-except
        try:
            raw_predictions = model.predict(data)
        except Exception:
            _handle_serving_error(
                error_message=(
                    "Encountered an unexpected error while evaluating the model. Verify"
                    " that the serialized input Dataframe is compatible with the model for"
                    " inference."
                )
            )

        predictions = get_jsonable_obj(raw_predictions, pandas_orientation="records")
        result = json.dumps(predictions, cls=NumpyEncoder)
        return flask.Response(response=result, status=200, mimetype="application/json")

    return app


def ndarray2list(ndarray):
    """
    Convert n-dimensional numpy array into nested lists and convert the elements types to native
    python so that the list is json-able using standard json library.
    :param ndarray: numpy array
    :return: list representation of the numpy array with element types convereted to native python
    """
    if len(ndarray.shape) <= 1:
        return [x.item() for x in ndarray]
    return [ndarray2list(ndarray[i, :]) for i in range(0, ndarray.shape[0])]


def get_jsonable_obj(data, pandas_orientation=None):
    """Attempt to make the data json-able via standard library.
    Look for some commonly used types that are not jsonable and convert them into json-able ones.
    Unknown data types are returned as is.

    :param data: data to be converted, works with pandas and numpy, rest will be returned as is.
    :param pandas_orientation: If `data` is a Pandas DataFrame, it will be converted to a JSON
                               dictionary using this Pandas serialization orientation.
    """
    if isinstance(data, np.ndarray):
        return ndarray2list(data)
    if isinstance(data, pd.DataFrame):
        return data.to_dict(orient=pandas_orientation)
    if isinstance(data, pd.Series):
        return pd.DataFrame(data).to_dict(orient=pandas_orientation)
    else:  # by default just return whatever this is and hope for the best
        return data


class NumpyEncoder(JSONEncoder):
    """ Special json encoder for numpy types.
    Note that some numpy types doesn't have native python equivalence,
    hence json.dumps will raise TypeError.
    In this case, you'll need to convert your numpy types into its closest python equivalence.
    """

    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, np.generic):
            return np.asscalar(o)
        return JSONEncoder.default(self, o)
