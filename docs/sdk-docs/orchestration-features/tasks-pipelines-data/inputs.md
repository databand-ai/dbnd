---
"title": "Task Inputs"
---
Every Task parameter at DBND can represent an input or an output.  When a DBND task starts to run, DBND automatically loads data from the location defined as task input from file systems into the proper in-memory format. When the task ends, DBND will persist task outputs in the desired file system.

We'll use this `train_model` task to describe how inputs and outputs work.

``` python
@task
def train_model(
    training_set: DataFrame, alpha: float = 0.5, l1_ratio: float = 0.5,
) -> ElasticNet:
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
    lr.fit(training_set.drop(["quality"], 1), training_set[["quality"]])
    return lr
```


## Loading Data into Tasks

If you need to quickly test pipelines on different sets of data, you can easily load new data into a task.

Running `train_model` will load a specified file type into the `training_set` DataFrame. By default, the type is resolved using the file extension, so `.csv` or `.parquet` will work.

Running `train_model` from the command line:

```bash
dbnd run train_model --training-set  dbnd-examples/data/wine_quality.csv.gz
```

Running `train_model` as part of a pipeline:

```python
train_model.task(training_set="dbnd-examples/data/wine_quality.csv.gz").dbnd_run()
``` 

Multiple sources can be loaded as DataFrame. Check `fetch_data` task in ['Predict Wine Quality' example](doc:orchestration-examples) to see how partitioned data is dynamically loaded into a task input as a single DataFrame. 

If you work with a file directly (you don't want DBND to autoload it for you), use DBND's `Target` or python 'Path' type to represent file system independent paths:

```python
@task
def read_data(path: Path) -> int:
    num_of_lines = len(open(path, "r").readlines())
    return num_of_lines
```

When task input is a string, you need to explicitly indicate that some value in the command line is the path:

```python
@task
def prepare_data(data: str) -> str:
    return data
```

```bash
#value is path
dbnd run prepare_data --set data=@/path_to_file_to_load

#value is **string**
dbnd run prepare_data --set data=my_string_value
```
 
A `@task` decorator can also be used to configure how data is loaded. For example, if your input file is tab-delimited, you can configure it as follows:

```python
@task(data=parameter[DataFrame].csv.load_options(FileFormat.csv, sep="\t"))
def prepare_data(data: DataFrame) -> DataFrame:
    data["new_column"] = 5
    return data
```

This way, Pandas parameters for [read_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html) method can be provided.
  
To specify that your input is of a specific type, regardless of its file extension, use:
```bash
dbnd run train_model --set training-set=myfile.xyz --train-model-training-set--target csv
```