---
"title": "Tracking Redshift"
---
Databand allows you to log your dataset operations when using Python to call SQL commands on Redshift. Wrapping your Redshift cursor's execution with Databand's RedshiftTracker context manager will catch the cursor's result and extract [Dataset Tracking](doc:dataset-logging)  operations from it. Currently, only `COPY INTO` is supported.

# Requirements
This guide assumes that your Redshift is configured to accept inbound connections. We will be using `psycopg2` to connect to Redshift.  Currently, DBND only supports `psycopg2` connections as the connection parameter.

Make sure that the package "dbnd-redshift" is installed (via `pip install databand[redshift]`, for example). See more info at [Installing DBND](doc:installing-dbnd)


# Integration with RedshiftTracker

Assume the following code is what you are currently using to copy some files into a Redshift table:
<!-- noqa -->
```python
import psycopg2

SQL_QUERY = """
COPY DB.PUBLIC.TABLE
FROM s3://path/to/some/file/file.csv
iam_role '<role>' csv;
"""

with psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
) as con:
    c = con.cursor()
    c.execute(SQL_QUERY)
```

To log the results of your query with Databand, you should run all your SQL queries in the context of  `RedshiftTracker`:
<!-- noqa -->
```python
from dbnd_redshift import RedshiftTracker

with RedshiftTracker():
        ...
        c.execute(SQL_QUERY)
        ...
```

Under the hood, RedshiftTracker will catch the execution of c.execute(SQL_QUERY). Only one query execution should be provided for each RedshiftTracker context.


## COPY INTO Command
Databand can track "COPY INTO" command. This will allow you to track both the read operation of your file from S3 as well as the write operation to DB.PUBLIC.TABLE in Redshift.

![](https://files.readme.io/25cf459-Screen_Shot_2022-01-10_at_14.13.15.png)

Current COPY INTO Limitations are:
1. Schema tracking is only supported for tables, not files.
2. Nested queries are not supported (e.g.: COPY (SELECT * FROM TABLE) table FROM...).


## Tracking Schema and Column Statistics with RedshiftTracker
To track the schema and column level statistics of the copied data, users can provide a DataFrame to RedshiftTracker. Providing the tracker with a DataFrame will result in loading it into memory, which might have a performance impact with large DataFrames. In these cases it is advised to read a small portion of the DataFrame by using the `nrows` param as seen in the example below.

<!-- noqa -->
```python
import pandas as pd
from dbnd_redshift import RedshiftTracker
source_file_path = "s3://some/path/file.csv"
df = pd.read_csv(source_file_path, nrows=50) # <-- Partially reading the DataFrame
with RedshiftTracker() as tracker:
        ...
        tracker.set_dataframe(dataframe=df)  # <-- Log the DataFrame's metadata
        c.execute(SQL_QUERY)
        ...
```
