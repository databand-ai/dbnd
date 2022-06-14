---
"title": "Datasets"
---
# Using `dataset_op_logger`
Using Databand's `dataset_op_logger` context manager, you can log numerous attributes about the datasets being processed by your Python code including the dataset shape, schema, column-level stats, and whether the dataset operation was successful.

Examples:

<!-- noqa -->
```python
from dbnd import dataset_op_logger

### Read example
with dataset_op_logger(source_path, "read") as logger:
    df = read(path, ...)
    logger.set(data=df)

### Write example
df = Dataframe(data)

with dataset_op_logger(target_path, "write") as logger:
    write(df, ...)
    logger.set(data=df)
```

After adding the above snippets to your code, Databand will track these operations and send the associated metadata to the [Affected Datasets](doc:affected-datasets) tab of your pipeline run.

In order to report a dataset operation to Databand, you need to provide the following:
* **The path for your dataset.** Provide the full URI for your file, table, API, etc.

* **The type of operation.** Specify whether the operation is a `read` or a `write`.

* **The dataframe that contains the records being read or written in your operation.** This is what provides Databand visibility of your operation so that metadata can be collected.


## Dataset Paths
In order for Databand to track your datasets from different sources, you should provide URI paths in a consistent manner. This will help ensure that a dataset affected by multiple operations will be identified as the same dataset across tasks (i.e. some file is written by a task and then read downstream by a different task). Some recommendations for good URI formats are below. **URIs are case-sensitive**.

For standard file systems or object storage, provide a fully-qualified URI. For example:
* `file://data/local_data.csv`
* `s3://bucket/key/dataset.csv`
* `gs://bucket/key/dataset.csv`
* `wasb://containername@accountname.blob.core.windows.net/dataset_path`

For data warehouses, you should generally provide the hostname and/or region of your warehouse along with the path to your table. For example:
* `bigquery://region/project/dataset/table`
* `snowflake://name.region.cloud/database/schema/table`
* `redshift://host/database/schema/table`

## Dataset Operation Context
It’s crucial that you wrap only operation-related code with the `dataset_op_logger` context manager. Anything that’s not related to reading or writing your dataset should be placed outside the context so that unrelated errors do not potentially flag your dataset operation as having failed.

**Good Example:**

<!-- noqa -->
```python
from dbnd import dataset_op_logger

with dataset_op_logger("s3://path/to/file.csv", "read") as logger:
    value = read_from()
    logger.set(data=value)
    # Read is successful

unrelated_func()
```

**Bad Example:**

<!-- noqa -->
```python
from dbnd import dataset_op_logger

with dataset_op_logger("s3://path/to/file.csv", "read") as logger:
    value = read_from()
    logger.set(data=value)
    # Read is successful
    unrelated_func()
    # If unrelated_func raises an exception, a failed read operation is reported to Databand.
```

## Supported Dataset Types
### Native Support
By default, `dataset_op_logger` supports the logging of pandas dataframes.

To enable PySpark dataframe logging, install the `dbnd-spark` PyPI library on your client. No additional import statements are required in your PySpark script beyond `from dbnd import dataset_op_logger`.

### Lists of Dictionaries
When fetching data from an external API, you will often get data in the following form:
```python
data = [
  {
    "Name": "Name 1",
    "ID": 1,
    "Information": "Some information"
  },
  {
    "Name": "Name 2",
    "ID": 2,
    "Information": "Other information"
  },
  ...
]
```

Providing this list of dictionaries as the data argument to `dataset_op_logger` will allow you to report its schema and volume:

<!-- noqa -->
```python
from dbnd import dataset_op_logger

with dataset_op_logger("http://some/api/response.json", "read"):
    logger.set(data=data)
```

**Volume** will be determined by calculating the length of this list. In our example, the volume will be `2`.

**Schema** is going to be determined by flattening the dictionary. In our example, the schema is going to be: `Name: str`, `ID: int`, `Information: str`.

### Custom Dataframes
For information on logging custom dataframe types, see [Custom Value Type](doc:custom-value-type).

## Optional Parameters
In addition to the dataset path and operation type, `dataset_op_logger` also accepts certain optional parameters that can limit or enhance the metadata being logged in your operation. A list of those parameters, their default values, and their descriptions is below:

* **with_schema** (True) – Extracts the schema of the dataset so that you can view the column names and data types in Databand.

* **with_preview** (False) – Extracts a preview of your dataset so that it can be displayed in Databand. The number of records in the preview is dependent on the size of the data, but it generally equates to 10-20 preview records.

* **with_stats** (True)– Calculates column-level stats on your dataset. This includes numerous statistical measures such as distinct and null counts, averages, standard deviations, mins and maxes, and quartiles. **NOTE**: To enable column-level stats, `with_schema` cannot be set to False.

* **with_histograms** (False) – Generates bar graphs showing the distribution of values in each column.

* **with_partition** (False) – In the event that your datasets have partitioned file paths, you can use `with_partition=True` to ensure that the same dataset across partitions will resolve to a single dataset in Databand. For example, `s3://bucket/key/date=20220415/file.csv` and `s3://bucket/key/date=20220416/file.csv` would be interpreted as two distinct datasets by default in Databand. Enabling the `with_partition` parameter will ignore the partitions when parsing the dataset name so that you can easily track trends and set alerts across runs.

It is important to note that not all parameters are supported for every type of dataset object. For a breakdown of which parameters are supported by each type of object, please refer to the **Metadata Logged** section on the [Dataset Logging](doc:dataset-logging#metadata-logged) page.
[block:callout]
{
  "type": "danger",
  "title": "Impact on Performance",
  "body": "Please be aware that the `with_stats` and `with_histograms` parameters will add additional overhead to the performance time of your pipeline since every value in every column must be profiled. These parameters should be tested in a development environment first against datasets that are representative of the size of datasets in your production environment to ensure that the performance tradeoff is acceptable."
}
[/block]