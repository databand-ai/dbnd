---
"title": "Pipeline"
---
The following pipeline shows how data can be passed into and returned from tasks:

<!-- noqa -->
```python
from dbnd import pipeline
from pandas import DataFrame

@pipeline
def evaluate_data(raw_data: DataFrame):
    data = prepare_data(raw_data=raw_data)
    alpha = calculate_alpha()
    beta = calculate_beta()
    coefficient = calculate_coefficient(alpha, beta)

    return data, coefficient

```

Task dependencies are automatically created when `dbnd` inspects the source of the input of each task. If a task's input is dependent on another task's output -- `dbnd` will automatically set these dependent on one another.

Additionally, you can have nested pipelines:

<!-- noqa -->
```python
from dbnd import task, pipeline
from pandas import DataFrame

@task
def acquire_data(source):
    return source

@pipeline
def prepare_data_pipeline(source):
    raw_data = acquire_data(source)
    data, coefficient = evaluate_data(raw_data)
    return data, coefficient
```

As you can see from this example `prepare_data_pipeline` contains `evaluate_data` pipeline.


## Manual dependencies management.

If you require two tasks to be dependent but have no parameter chain, you can simply use the task's `set_upstream` or `set_downstream` methods, similar to the method used in Airflow DAGs.

For example:

<!-- noqa -->
```python
from dbnd import pipeline
@pipeline(result=("model", "metrics", "validation_metrics"))
def linear_reg_pipeline():
    training_set, testing_set = prepare_data()
    model = train_model(training_set)
    metrics = test_model(model, testing_set)
    validation_metrics = get_validation_metrics(testing_set)
    # return the model along with metrics
    return model, metrics, validation_metrics
```

Looking at the example above, we have a scenario where we want `get_validation_metrics` to be executed last, regardless of the actual dependencies. To achieve this, we can add:


<!-- noqa -->
```python
validation_metrics = get_validation_metrics(testing_set)
validation_metrics.task.set_upstream(metrics.task)
```
Now, the `validation_metrics` task is downstream to the `metrics` or `test_model` task, and will execute last.