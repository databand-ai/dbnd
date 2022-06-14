---
"title": "Accessing Run Results"
---
After a Run finishes, you might want to access the Run’s output inside the Python code, even if the Run was executed on a remote environment. This is the method to do it:
`run.load_from_result()`

If your Pipeline, Task Run, etc. has only one result, it receives the `RESULT_PARAM` name so you don’t need additional parameters to load it.
If you need to distinguish between different results, you have to specify the name of the output you want to load using:
`run.load_from_result(“output_name”)`

You can also specify the type that you want the output to be loaded into (`value_type` can be str, int, etc. with):
`run.load_from_result(“output_name”, value_type=str)`

For instance, let’s assume we have a `prepare_data_pipeline` Pipeline:

<!-- noqa -->
```python
from dbnd import pipeline
from pandas import DataFrame

@pipeline(result=("data", "validation"))
def prepare_data_pipeline(expected_data: DataFrame, raw_data: str):
    treated_data = prepare_data(raw_data)
    validation = validate_data(expected_data, treated_data)

    return treated_data, validation
```


To run it from within Python code, use:

<!-- noqa -->
```python
run = prepare_data_pipeline.dbnd_run(task_version=now())
```

Now, to access the results, use:

<!-- noqa -->
```python
data = run.load_from_result("data", value_type=DataFrame)
validation = run.load_from_result("validation")
```