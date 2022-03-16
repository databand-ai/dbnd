---
"title": "Orchestration Quickstart Tutorial"
---
In this tutorial, you will learn how to perform the following tasks:

1. Create a pipeline for use with your data.
2. Run a pipeline and learn how to view the results.
3. Learn how to modify your pipeline so that you can modify the future runs easily.

## Creating a Task

After you have successfully installed DBND, you can create a new task, run it, and change its inputs.

### Step 1: Create a Task

* Navigate to the root directory of your DBND project (the root directory is the one with the `project.cfg` file) via the command line.

* In the root directory, create a new file called `example.py` and open it using your text editor/IDE. Let's add a simple Python function with a DBND `@task` decorator:

```python
from dbnd import task

@task
def calculate_alpha(alpha: float = 0.5) -> float:
    alpha += 0.1
    return alpha
```

* Save your file.

### Step 2: Run a Task

* Now that you have created a task let us run it. To run the task via CLI, use:

```
dbnd run example.calculate_alpha
```

* You will see a similar snippet near the bottom of the command line output:

```
PARAMS:    : 
Name    Kind    Type    Format    Source    -= Value =-
alpha   param   float             default    0.5
result  output  float   .pickle              /Users/name/databand/data/dev/2021-08-02/calculate_alpha/calculate_alpha_d869b45637/result.txt :='0.6'

[Text omitted for brevity]

====================
= Your run has been successfully executed!
```

### Step 3: Iterate Task Inputs

DBND allows you to change your task inputs from the CLI. Changing these values affects the output of your tasks.

* In your task, the `calculate_alpha` function has a default value for the alpha parameter. You can override it using the CLI:

```bash
dbnd run example.calculate_alpha --set-root alpha=0.4
```

* The results displayed should include the following snippet:

```
PARAMS:    :
Name    Kind    Type    Format    Source            -= Value =-
alpha   param   float             ctor               0.4
result  output  float   .pickle                      /Users/name/databand/data/dev/2021-08-02/calculate_alpha/calculate_alpha_d869b45637/result.txt :='0.5'
=
====================
```

## Creating a Pipeline

In this section, you will create a pipeline using some example code.

### Pipelines 101

A pipeline is a series of tasks wired together to run in some order.

In the following sections, you will work with a pipeline that produces an ML model that predicts different wine quality.

### Step 1: Look at the Initial Code Workflow

The following is the initial workflow code before any modifications with DBND. The process includes the following tasks:

Task 1: **Input Data** - loading a data file containing the wines and their attributes
Task 2: **Data Preparation** - splitting the data set into distinct training and validation sets
Task 3: **Model Training** - creating an ElasticNet model based on the training data set
Task 4: **Model Validation** - testing the model with test data to create performance metrics

In addition to these four tasks, the workflow includes two input parameters that have been hardcoded.

```python
import pandas as pd
import numpy as np
import logging
from sklearn.linear_model import ElasticNet
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

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
```

### Step 2: Adapt the Workflow with DBND

Using DBND in the workflow involves the following steps:

Step 1. Functionalize the parts of the code that you want to modularize into tasks
Step 2. Assign `@task` decorators to define each function as a task
Step 3. Use the `@pipeline` decorator to define the structure of the pipeline

```python
# python3.6
import numpy as np
import logging
from pandas import DataFrame
from dbnd import task, pipeline
from sklearn.linear_model import ElasticNet
from dbnd import log_dataframe, log_metric
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from typing import Tuple
from sklearn.model_selection import train_test_split

logging.basicConfig(level=logging.INFO)

@task(result="training_set, validation_set")
def prepare_data(raw_data: DataFrame) -> Tuple[DataFrame, DataFrame]:
    train_df, validation_df = train_test_split(raw_data)

    return train_df, validation_df

@task
def train_model(
    training_set: DataFrame,
    alpha: float = 0.5,
    l1_ratio: float = 0.5,
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
    raw_data: DataFrame,
    alpha: float = 0.5,
    l1_ratio: float = 0.5,
):
    training_set, validation_set = prepare_data(raw_data=raw_data)

    model = train_model(
        training_set=training_set, alpha=alpha, l1_ratio=l1_ratio
    )
    validation = validate_model(model=model, validation_dataset=validation_set)

    return model, validation
```

Pay attention to the following changes in the modified code above:

1. Each task has a `@task` decorator with input and output parameters.
2. The pipeline has a `@pipeline` decorator with defined input and output parameters.
3. The `result` keyword at the decorator is used for defining names for outputs of the task.
4. The input data (in this case, a CSV file) is no longer a part of the code. Instead, it is an attribute that's entered and is adjustable as a parameter.
5. All variable parameters have been defined as pipeline attributes.
6. Logging and metrics APIs have been added to the `model_validation` task to track performance.

### Step 3: Create the Wine Quality Pipeline

* Using the text editor/IDE of your choice, begin by creating a new Python [module](https://docs.python.org/3/tutorial/modules.html). You can call it with the name of your choice (e.g., `predict_wine_quality`).

* Copy and save the DBND code provided on this page into your module.

## Running a Pipeline with Data

* Create a new file called `wine.csv` and copy in the [sample data](https://www.pastiebin.com/5bafbb27420c3) we've provided.

* Once you've prepared your data file, you can run your pipeline with the CLI:

```bash
dbnd run predict_wine_quality.predict_wine_quality --set raw_data=wine.csv
```

Note that this command reflects the module and data file that is saved in your DBND directory; otherwise, you will need to provide full file names.

## Running a Pipeline with Varying Inputs

You can run a pipeline with different input data and/or parameters by specifying a different data file or parameter as part of the `dbnd run` command.

### Re-Run Your Pipeline with a Different Data File

To re-run your pipeline with a different data file, duplicate your original `wine.csv` file and call it `wine2.csv`.

In the command line, run:

```bash
dbnd run predict_wine_quality.predict_wine_quality --set raw_data=wine2.csv
```

DBND will run the pipeline using the new data file.

### Re-Run Your Pipeline with Different Parameter Values

You can change the parameter values for a run by specifying new values as part of the `dbnd run` command. For example, if you wanted to set the values for `alpha` and `l1_ratio` to 0.7, you would run the following:

`dbnd run predict_wine_quality.predict_wine_quality --set raw_data=wine.csv --set l1_ratio=0.7 --set alpha=0.7`

DBND will run the pipeline using the values provided as the net input parameters.