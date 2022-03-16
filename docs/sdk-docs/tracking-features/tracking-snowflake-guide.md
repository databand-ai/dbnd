---
"title": "Tracking Snowflake"
---
Databand allows you to log your dataset operations when using Python to call `COPY INTO` SQL commands on Snowflake. Wrapping your Snowflake cursor's execution with Databand's `SnowflakerTracker` context manager will catch the cursor's result and extract dataset operations from it.

# 1. Installation

Begin by installing the required packages using the following command:
```bash
pip install dbnd dbnd-snowflake
```

> ℹ️ Requirements
> Although there are many alternatives for writing data to Snowflake using Python, logging your Snowflake dataset operations with Databand requires usage of Snowflake's official Python package: `snowflake-connector-python`.

# 2. Integration with SnowflakeTracker

Assume the following code is what you are currently using to copy some file into a Snowflake table:

<!-- noqa -->
```python
from snowflake import connector

SQL_QUERY = """
COPY INTO DB.PUBLIC.TABLE
FROM s3://path/to/some/file/file.csv
CREDENTIALS = (AWS_KEY_ID = 'key' AWS_SECRET_KEY = 'key');
"""

with connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA) as con:
    c = con.cursor()
    c.execute(SQL_QUERY)
```


To log the results of `SQL_QUERY` with Databand, you can make the following changes:
<!-- noqa -->
```python
from snowflake import connector
from dbnd_snowflake import SnowflakeTracker

SQL_QUERY = """
COPY INTO DB.PUBLIC.TABLE
FROM s3://path/to/some/file/file.csv
CREDENTIALS = (AWS_KEY_ID = 'key' AWS_SECRET_KEY = 'key');
"""

with connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA) as con:
    c = con.cursor()
    with SnowflakeTracker():
        c.execute(SQL_QUERY)
```

Under the hood, `SnowflakeTracker` will catch the execution of `c.execute(SQL_QUERY)`. This will allow you to track both the read operation of your file from S3 as well as the write operation to `DB.PUBLIC.TABLE` in Snowflake.

![SnowflakeTracker.png](https://files.readme.io/9194509-SnowflakeTracker.png)


## Limitations

- Schema tracking is only supported for tables, not files.
- Only one query execution should be provided for each `SnowflakerTracker` context.
- Nested queries are not supported (e.g.: `COPY INTO (SELECT * FROM TABLE) table FROM...`).
