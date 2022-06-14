---
"title": "Task Inputs"
---
Every Task parameter at DBND can represent an input or an output.  When a DBND task starts to run, DBND automatically loads data from the location defined as task input from file systems into the proper in-memory format. When the task ends, DBND will persist task outputs in the desired file system.

We'll use this `prepare_data` task to describe how inputs and outputs work.

```python
from dbnd import task
from pandas import DataFrame

@task
def prepare_data(data: DataFrame, key: str) -> str:
    result = data.get(key)
    return result
```


## Loading Data into Tasks

If you need to quickly test pipelines on different sets of data, you can easily load new data into a task.

Running `prepare_data` will load a specified file type into the `data` DataFrame. By default, the type is resolved using the file extension, so `.csv` or `.parquet` will work.

Running `prepare_data` from the command line:

```bash
dbnd run prepare_data --set data=dbnd-examples/data/wine_quality.csv.gz
```

Running `prepare_data` as part of a pipeline:

<!-- noqa -->
```python
prepare_data.task(data="dbnd-examples/data/wine_quality.csv.gz").dbnd_run()
```

Multiple sources can be loaded as DataFrame. Check `fetch_data` task in ['Predict Wine Quality' example](doc:orchestration-examples) to see how partitioned data is dynamically loaded into a task input as a single DataFrame.

If you work with a file directly (you don't want DBND to autoload it for you), use DBND's `Target` or python 'Path' type to represent file system independent paths:

```python
from dbnd import task
from targets.types import Path

@task
def read_data(path: Path) -> int:
    num_of_lines = len(open(path, "r").readlines())
    return num_of_lines
```

When task input is a string, you need to explicitly indicate that some value in the command line is the path:

```python
from dbnd import task

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
from dbnd import task, parameter
from pandas import DataFrame
from targets.target_config import FileFormat

@task(data=parameter[DataFrame].csv.load_options(FileFormat.csv, sep="\t"))
def prepare_data(data: DataFrame) -> DataFrame:
    data["new_column"] = 5
    return data
```

This way, Pandas parameters for [read_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html) method can be provided.

To specify that your input is of a specific type, regardless of its file extension, use:
```bash
dbnd run prepare_data --set data=myfile.xyz --prepare-data-data--target csv
```