---
"title": "Python Quickstart"
---
In this tutorial, you will learn how to:
* Log data, metrics, outputs, and other important details for tracking your data pipeline health in a simple ML workflow with Pandas DataFrames
* Use DBND's histogram feature to quickly visualize DataFrame metadata in your CLI

The tutorial uses a Python script that trains a model to predict disease progression from a healthcare dataset.
# Installing DBND

Before starting with this tutorial, install `dbnd`. In order to run the example, you should also install `scikit-learn`, and `pandas` using the following command:
``` bash
pip install dbnd scikit-learn pandas
```

# Step 1. Loading and Tracking the Dataset
Sci-Kit Learn provides many datasets for experimentation. Among them is the Diabetes dataset. The Diabetes dataset has ten features:
* age (`age`)
* sex (`sex`)
* body mass index (`bmi`)
* average blood pressure (`bp`)
* six blood serum measurements (`s1 - s6`) collected from 442 patients.

The target (`target`) is a quantitative measurement of disease progression one year after the indicators were collected.

Note that in this dataset, each of the ten feature variables has been mean centered and scaled by the standard deviation times `n_samples` (i.e., the sum of squares of each column totals `1`).

First, load this dataset, then split it into a training set and testing set for our model:
``` python
# Python 3.6.8
from sklearn import datasets
from sklearn.model_selection import train_test_split
from pandas import DataFrame, Series
from typing import Tuple

def prepare_data() -> Tuple[DataFrame, DataFrame]:
	''' load dataset from sklearn. split into training and testing sets'''
	raw_data = datasets.load_diabetes()

	# create a pandas DataFrame from sklearn dataset
	df = DataFrame(raw_data['data'], columns=raw_data['feature_names'])
	df['target'] = Series(raw_data['target'])

	# split the data into training and testing sets
	training_data, testing_data = train_test_split(df, test_size=0.25)

	return training_data, testing_data
```

As is, there is no good method to quickly understand this data. Are there biases in the data? What is the statistical composition? To answer these questions, you can track statistics, metadata, and visualize your data composition with DBND logging.

## Integrating DataFrame Tracking
``` python
# Python 3.6.8
from sklearn import datasets
from sklearn.model_selection import train_test_split
from pandas import DataFrame, Series
import logging
from typing import Tuple
from dbnd import log_dataframe, log_metric

logging.basicConfig(level=logging.INFO)

def prepare_data() -> Tuple[DataFrame, DataFrame]:
	''' load dataset from sklearn. split into training and testing sets'''
	raw_data = datasets.load_diabetes()

	# create a pandas DataFrame from sklearn dataset
	df = DataFrame(raw_data['data'], columns=raw_data['feature_names'])
	df['target'] = Series(raw_data['target'])

	# split the data into training and testing sets
	training_data, testing_data = train_test_split(df, test_size=0.25)

	# use DBND logging features to log DataFrames with histograms
	log_dataframe("training data", training_data,with_histograms=True,
	            with_schema=True, with_stats=True)
	log_dataframe("testing_data", testing_data)

	# use DBND logging features to log the mean of s1
	log_metric("mean s1", training_data['s1'].mean())

	return training_data, testing_data
```

`log_dataframe("training data", training_data, with_histograms=True, with_schema=True, with_stats=True)` allows us to track the Pandas DataFrame object.

By setting our parameters `with_histograms=True`, `with_schema=True`, and `with_stats=True`, you capture important statistics and metadata describing the DataFrame. The data displayed by the histogram can be further customized.
See also [Tracking Histograms](doc:histogram) for more customization options.

## Integrating Metric Tracking

`log_metric` allows logging meaningful metrics of our data. For example, if you want to find the mean of the `s1` column, you can add `log_metric("mean s1", training_data['s1'].mean())`. `log_metric` can also be used to log larger data structures, such as a `numpy` array or Pandas `Series`.
# Step 2. Training a Model and Tracking Metrics
Now that the data is ready, you can start to train and test a `LinearRegression` model to predict patient disease progression.
``` python
# Python 3.6.8
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from pandas import DataFrame, Series
import logging
from typing import Tuple
from dbnd import log_dataframe
from sklearn.linear_model import LinearRegression

logging.basicConfig(level=logging.INFO)

def prepare_data() -> Tuple[DataFrame, DataFrame]:
	''' load dataset from sklearn. split into training and testing sets'''
	pass

def train_model(training_data: DataFrame) -> LinearRegression:
	''' train a linear regression model '''
	model = LinearRegression()

	# train a linear regression model
	model.fit(training_data.drop('target', axis=1), training_data['target'])
	return model

def test_model(model: LinearRegression, testing_data:DataFrame) -> str:
	''' test the model, output mean squared error and r2 score'''
	testing_x = testing_data.drop('target', axis=1)
	testing_y = testing_data['target']
	predictions = model.predict(testing_x)

	mse = mean_squared_error(testing_y, predictions)
	r2_score = model.score(testing_x, testing_y)

	return f"MSE: {mse}, R2: {r2_score}"
```

For an ML workflow like this, it's good practice to log our model's performance metrics. In addition, it is helpful to log important attributes of our model. For example, in a `LinearRegression` model you may want to evaluate the coefficients and intercept of the model and/or log them.

## Tracking More Metrics
``` python
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from pandas import DataFrame, Series
import logging
from typing import Tuple
from dbnd import log_dataframe, log_metric
from sklearn.linear_model import LinearRegression

logging.basicConfig(level=logging.INFO)

def prepare_data() -> Tuple[DataFrame, DataFrame]:
	''' load dataset from sklearn. split into training and testing sets'''
	raw_data = datasets.load_diabetes()

	# create a pandas DataFrame from sklearn dataset
	df = DataFrame(raw_data['data'], columns=raw_data['feature_names'])
	df['target'] = Series(raw_data['target'])

	# split the data into training and testing sets
	training_data, testing_data = train_test_split(df, test_size=0.25)

	# use DBND logging features to log DataFrames with histograms
	log_dataframe("training data", training_data,with_histograms=True,
                with_schema=True, with_stats=True)
	log_dataframe("testing_data", testing_data)

	# use DBND logging features to log the mean of s1
	log_metric("mean s1", training_data['s1'].mean())

	return training_data, testing_data

def train_model(training_data: DataFrame) -> LinearRegression:
	''' train a linear regression model '''
	model = LinearRegression()

	# train a linear regression model
	model.fit(training_data.drop('target', axis=1), training_data['target'])

	# use DBND log crucial details about the regression model with log_metric:
	log_metric('model intercept', model.intercept_) # logging a numeric
	log_metric('coefficients', model.coef_) # logging an np array
	return model

def test_model(model: LinearRegression, testing_data:DataFrame) -> str:
	''' test the model, output mean squared error and r2 score'''
	testing_x = testing_data.drop('target', axis=1)
	testing_y = testing_data['target']
	predictions = model.predict(testing_x)
	mse = mean_squared_error(testing_y, predictions)
	r2_score = model.score(testing_x, testing_y)

	# use DBND log_metric to capture important model details:
	log_metric('mean squared error:', mse)
	log_metric('r2 score', r2_score)

	return f"MSE: {mse}, R2: {r2_score}"

if __name__ == '__main__':
    training_set, testing_set = prepare_data()
    model = train_model(training_set)
    metrics = test_model(model, testing_set)
```

DBND’s `log_metric` function can handle a variety of data types and data structures. In this case, you are logging a `float` for the model's intercept and a `numpy array` for the model's coefficients. After testing our model, you also log pertinent performance metrics such as the mean squared error and R2 score in the same manner.

> ℹ️**Tracking Functions**
> DBND allows you to track functions from different file locations. Visit [Tracking Functions](https://docs.databand.ai/docs/functions#tracking-functions-with-decorators) for more information.
# Step 3. Running the Script

Before running the script, you need to enable tracking of Python scripts by exporting the `DBND__TRACKING` environment variable:
``` bash
$export DBND__TRACKING=True
```

Now, you can execute the Python script normally:
``` bash
$python linear_regression.py
```

The output will be all of the tracked data, including histograms, DataFrame statistics, and metrics.

```bash
====================
= Running Databand!
 TRACKERS   : ['console']
...

INFO  linear_regression.py__d98fd8fb0a - Histogram logged: training data.target
INFO  linear_regression.py__d98fd8fb0a - ###############################################################################
INFO  linear_regression.py__d98fd8fb0a - ███████                                                   5  25.0
INFO  linear_regression.py__d98fd8fb0a - ████████████████████████████████████                     23  41.05
INFO  linear_regression.py__d98fd8fb0a - ██████████████████████████████████████████████████       32  57.1
INFO  linear_regression.py__d98fd8fb0a - █████████████████████████████████████                    24  73.15
INFO  linear_regression.py__d98fd8fb0a - ███████████████████████████████████████████████████████  35  89.2
INFO  linear_regression.py__d98fd8fb0a - ██████████████████████████                               17  105.25
INFO  linear_regression.py__d98fd8fb0a - ██████████████████████████████████                       22  121.30000000000001
INFO  linear_regression.py__d98fd8fb0a - ████████████████████████████████████████                 26  137.35000000000002
INFO  linear_regression.py__d98fd8fb0a - ████████████████████                                     13  153.4
INFO  linear_regression.py__d98fd8fb0a - ███████████████████████████████████████                  25  169.45000000000002
INFO  linear_regression.py__d98fd8fb0a - ██████████████████████████                               17  185.5
INFO  linear_regression.py__d98fd8fb0a - ████████████████████                                     13  201.55
INFO  linear_regression.py__d98fd8fb0a - ███████████████████████                                  15  217.60000000000002
INFO  linear_regression.py__d98fd8fb0a - ████████████████████████████                             18  233.65
INFO  linear_regression.py__d98fd8fb0a - ███████████████████████                                  15  249.70000000000002
INFO  linear_regression.py__d98fd8fb0a - ██████████████████████████                               17  265.75
INFO  linear_regression.py__d98fd8fb0a - ███████                                                   5  281.8
INFO  linear_regression.py__d98fd8fb0a - ██████                                                    4  297.85
INFO  linear_regression.py__d98fd8fb0a - ███                                                       2  313.90000000000003
INFO  linear_regression.py__d98fd8fb0a - ████                                                      3  329.95

...

INFO linear_regression.py__d98fd8fb0a - Metric logged: mean s1=0.00016865
INFO linear_regression.py__d98fd8fb0a - Metric logged: model intercept=154.34022510700007
INFO  linear_regression.py__d98fd8fb0a - Metric logged: coefficients=[ -10.29867519 -268.24113834  522.7893521   310.80055726 -908.43288433
  517.96198994  120.04400416  248.71644549  805.18877544   52.55633119]
INFO linear_regression.py__d98fd8fb0a -Metric logged: mean squared error:=2572.1155664746416
INFO  linear_regression.py__d98fd8fb0a - Metric logged: r2 score=0.49160156291981305
INFO linear_regression.py__d98fd8fb0a - Task linear_regression.py has been completed!
INFO linear_regression.py__d98fd8fb0a - Task dbnd_driver__d9b2d516f3 has been completed!
INFO  linear_regression.py__d98fd8fb0a -

====================
= Your run has been successfully executed!
 TRACKERS   : ['console']
```

> ℹ️ **Enabling Tracking**
> For more details on how to enable tracking, visit the [Tracking Python Scripts](https://docs.databand.ai/docs/python-scripts) page.

[block:html]
{
  "html": "<style>\n  pre {\n      border: 0.2px solid #ddd;\n      border-left: 3px solid #c796ff;\n      color: #0061a6;\n  }\n\n.CodeTabs_initial{\n  /* box shadows with with legacy browser support - just in case */\n    -webkit-box-shadow: 0 10px 6px -6px #777; /* for Safari 3-4, iOS 4.0.2 - 4.2, Android 2.3+ */\n     -moz-box-shadow: 0 10px 6px -6px #777; /* for Firefox 3.5 - 3.6 */\n          box-shadow: 0 10px 6px -6px #777;/* Opera 10.5, IE 9, Firefox 4+, Chrome 6+, iOS 5 */\n  }\n</style>"
}
[/block]
