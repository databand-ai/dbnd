---
"title": "Spark Input and Outputs (DataFrame)"
---
Spark inline tasks load spark DataFrames automatically by providing the right configuration to Spark DataFrameReader/Writer objects. 

> Note that the **physical read is done directly by Spark**. DBND will map data loading/saving into standard pyspark calls.  While running Spark DBND  supports `s3`,  `gs` protocols only if your Spark Instance supports direct read for these storage types.

By default, the data is loaded using [default read/write option set by Spark](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html).  

CSV file is the only exception, where the default read option is:
* `header = True`
* `inferSchema = True`

The default write configuration is `header=True`.

To perform an override, use `save_options` and `load_options` methods of parameter builder as in the example below:

```python
from targets.target_configx import FileFormat
@spark_task(result=output.save_options(FileFormat.csv, header=True)[spark.DataFrame])
def prepare_data(
    data=parameter.load_options(FileFormat.csv, header=False, sep="\t")[spark.DataFrame]
):
    print(data.show())
    return data
```

Supported file formats include:
* `FileFormat.txt`
* `FileFormat.parquet`
*  `FileFormat.json`

### How to use your own code to read/write Spark
If you want to use your own way of reading/writing Spark, see following example:
``` python
@spark_task
def prepare_data(data_path=parameter[PathStr]):
    df =   get_spark_session()read.format("csv").options(header=False, sep="\t").load(data_path)
    print(data.show())
    return data
```

### Known limitations
* For Azure Blob Store, your Spark instance should support the `wasb` protocol. DBND will translate the `HTTPS` protocol to `wasb` for Spark tasks automatically.
* You can't use dbnd functionality for Spark DataFrame read/write in case you need basePath parameter or `overwrite` mode changes.