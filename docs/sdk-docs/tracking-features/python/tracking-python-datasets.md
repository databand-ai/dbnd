---
"title": "Datasets"
---
# Using `dataset_op_logger`
You can log datasets by using the `with dataset_op_logger...` context. With this approach, you don’t need to specify if the operation was successful because its status is already being tracked inside the context. See [Dataset Logging](doc:dataset-logging)

Example:
<!-- xfail -->
```python
from dbnd import dataset_op_logger

### Read example
with dataset_op_logger(path, "read") as logger:
    df = read(path, ...)
    logger.set(data=df)

### Write example
df = Dataframe(data)
with dataset_op_logger(path, "write") as logger:
    write(df, ...)
    logger.set(data=df)
```

After adding the above snippet to your code, Databand will start tracking your dataset operations. The dataset tracking information can be found on the [Affected Datasets](doc:affected-datasets) Page.

Once inside the `dataset_op_logger` context, use the `set` command to provide the data frame either produced by the `read` operation or used in the `write` operation.

In order to report dataset operation to Databand, you need to provide the following:
* **The path for your dataset.** Provide the full URI (as complete as possible) with the schema, host, region, etc.
* **The type of operation.** Specify whether the operation is a `read` or a `write`.
* **The dataframe (Spark, pandas, or [other](doc:dataset-logging#support-for-custom-dataframe-types)) that contains the records being read or written in your operation.** This is what provides Databand visibility of your operation so that metadata can be collected such as row counts, schemas, histograms, etc.


## Dataset URI (path)
In order for Databand to track your datasets from different sources, you should provide URI paths in a consistent manner. This will help ensure that a dataset affected by multiple operations will be identified as the same dataset across tasks (i.e. some file is written by a task and then read downstream by a different task). Some recommendations for good URI formats are below. URIs are case-sensitive. This can potentially lead to Databand identifying two instances of the same URI as two different datasets.

For example, `s3://my_bucket/my_dataset` is a good URI, while  `s3://my_bucket/my_dataset/datetime=20200101T201020/file.csv` is most likely a bad URI, as it will have updated date at `datetime=20200101T201020` on every script execution

For standard filesystems or object, storages provide a Full URI.   For example: `file:///data/local_data.csv`, `s3://bucket/key/dataset.csv`, `gs://bucket/key/dataset.csv`, `wasb://containername@accountname.blob.core.windows.net/dataset_path`

For BigQuery you can use `bigquery://region/project/dataset/table`, for snowflake `snowflake://name.region.cloud/database/schema/table` and so on.

## Dataset Operation Context
It’s crucial that you wrap only operation-related code in the `dataset_op_logger` context. Anything that’s not related to reading or writing your dataset should be placed outside the context so that unrelated errors do not potentially flag your dataset operation as having failed.

**Good Example:**
<!-- xfail -->
```python
from dbnd import dataset_op_logger

with dataset_op_logger("file://path/to/value.csv", "read") as logger:
    value = read_from()
    logger.set(data=value)
    # Read is successful

unrelated_func()
```

**Bad Example:**
<!-- xfail -->
```python
from dbnd import dataset_op_logger

with dataset_op_logger("file://path/to/value.csv", "read") as logger:
    value = read_from()
    logger.set(data=value)
    # Read is successful
    unrelated_func()
    # If unrelated_func raises an exception, a failed read operation is reported to Databand.
```

## Supported Dataset Types
By default, `dbnd` supports pandas data frame logging.

To enable pySpark `DataFrame` logging, install the `dbnd-spark` PyPI library on your client. No additional import statements are required in your Python script.

Histograms and descriptive statistics calculations may impact the performance of your code.


## Histogram Reporting
Histograms for the columns of your datasets can be logged using the dataset logging feature. To enable histogram reporting, use `with_histograms=True` with `log_dataset_op` or `dataset_op_logger` . Your histograms along with a few key data profiling metrics will be accessible within the Databand UI.

<!-- xfail -->
```python
from dbnd import log_dataset_op
from dbnd._core.constants import DbndDatasetOperationType


log_dataset_op("location://path/to/value.csv", DbndDatasetOperationType.read,
               data=pandas_data_frame,
               with_histograms=True)
```

 Enable Databand to log and display histograms in config. To do this, set the `[tracking]/log_histograms` flag to `true`. For more information about these options, see [SDK Configuration](doc:dbnd-sdk-configuration).

## Descriptive Statistics Reporting
While histograms calculation may seriously impact your pipeline performance, descriptive statistics can be calculated much faster. This calculation is enabled by default. To disable descriptive stats reporting, use `with_stats=False`. Statistics are calculated only for numeric and string columns. Following metrics are reported: `min`, `max`, `records_count`, `mean`, `stddev`, `null_count` and quartiles — 25%, 50%, 75%.

Histograms calculation at the moment is only supported for Pandas dataframes.
Descriptive statistics calculation supported for both Pandas and Spark DataFrames.


### Dataset as List of Dictionaries (`List[Dict]`)
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

Providing this list of dictionaries as the data argument for the dataset logging function will allow you to report its schema and its volume:
<!-- xfail -->
```python
from dbnd import dataset_op_logger

with dataset_op_logger("http://some/api/response.json", "read"):
    logger.set(data=data)
```

**Volume** is going to be determined by calculating the length of this list. In our example, the volume will be `2`.

**Schema** is going to be determined by flattening the dictionary. In our example, the schema is going to be: `Name: str`, `ID: int`, `Information: str`.

 For other data frame support, see the  [Custom Value Type](doc:custom-value-type)
