---
"title": "Orchestration Examples"
---
There is a project with multiple examples of DBND usage. You can find it at https://github.com/databand-ai/dbnd/blob/master/examples.

> For orchestration use cases, see [DBND orchestration examples on GitHub](https://github.com/databand-ai/dbnd/blob/master/examples/src/dbnd_examples/orchestration)

## Predict Wine Quality

[This example ](https://github.com/databand-ai/dbnd/blob/master/examples/src/dbnd_examples/orchestration/examples/wine_quality/wine_quality_decorators_py3.py) shows a more complex version of the Predict Wine Quality tutorial to demonstrate:
*  conditional task invocation
*  a pipeline that calls another pipeline
*  custom logic to fetch partitioned production data.

We start with a basic ```predict_wine_quality``` pipeline.

Run a pipeline and set `data` parameter from the command line

```bash
dbnd run predict_wine_quality --set data=dbnd_examples_orchestration/data/wine_quality_minimized.csv
dbnd run predict_wine_quality_parameter_search  --set predict_wine_quality.data=dbnd_examples_orchestration/data/wine_quality_minimized.csv
```

Every parameter at any task in a pipeline can be set from the command line.

```bash
dbnd run predict_wine_quality --set validate_model.validation_dataset s3://databand-example/data/wine_quality.csv
```

Run pipeline with complex fetch data logic:

```bash
dbnd run predict_wine_quality  --env prod --set alpha=0.2 --set l1_ratio=0.4  --set fetch.data_period=30d
```

Our `predict_wine_quality` pipeline expects a pandas DataFrame as `data` parameter. Here we show how we build `data` DataFrame from complex production data. Observe that `fetch_data` pipeline is called from `predict_wine_quality` pipeline.

`fetch_data()` and `calculate_alpha()` are DBND tasks that are executed in specific conditions (lines 61 and 65)

We write another pipeline to perform a grid search and to invoke `predict_wine_quality` pipeline numerous times

## Syntax Tutorials and Examples
To find our more about the possible syntax for DBND tasks and pipelines, please take a look at [this GitHub folder](https://github.com/databand-ai/dbnd/blob/master/examples/src/dbnd_examples/orchestration/tutorial_syntax).
