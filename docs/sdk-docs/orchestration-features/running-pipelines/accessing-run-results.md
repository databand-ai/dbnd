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

For instance, let’s assume we have a `predict_wine_quality` Pipeline (we are using ‘Predict wine quality’ Pipeline quite often in our examples):

```python
@pipeline(result=("model", "validation"))
def predict_wine_quality(
    raw_data: DataFrame,
    alpha: float = 0.5,
    l1_ratio: float = 0.5,
):
    training_set, validation_set = prepare_data(raw_data=raw_data)
    model = train_model(training_set=training_set, alpha=alpha, l1_ratio=l1_ratio)

    validation = validate_model(model=model, validation_dataset=validation_set)

    return model, validation
```

To run it from within Python code, use: 
```python
run = predict_wine_quality.dbnd_run(task_verion=now())
```

Now, to access the results, use:

```python
model = run.load_from_result(“model”, value_type=ElasticNet)
validation = run.load_from_result(“validation”)
```