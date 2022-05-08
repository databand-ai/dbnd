---
"title": "Tracking Redshift"
---
Databand allows you to log your dataset operations when using Python to call SQL commands on Redshift. Wrapping your Redshift cursor's execution with Databand's `RedshiftTracker` context manager will catch the cursor's result and extract [Dataset Logging](doc:dataset-logging)  operations from it. Currently, only [COPY queries](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) are supported.

# Requirements
This guide assumes that your Redshift is configured to accept inbound connections. We will be using `psycopg2` to connect to Redshift.  Currently, DBND only supports `psycopg2` connections as the connection parameter.

Make sure that the `dbnd-redshift` package is installed (via `pip install databand[redshift]`, for example). See more info at [Installing DBND](doc:installing-dbnd).


# Integrating RedshiftTracker

Assume the following code is what you are currently using to copy files into a Redshift table:

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

To log the results of your query with Databand, wrap the execution of your cursor with the `RedshiftTracker` context manager:

```python
from dbnd_redshift import RedshiftTracker, RedshiftTrackerConfig

with RedshiftTracker(
    conf=RedshiftTrackerConfig(with_preview=True, with_stats=True, with_schema=True)
):
    c.execute(SQL_QUERY)
```

The above will capture the results of the executed query. Only one query execution should be provided for each `RedshiftTracker` context.

# Optional Parameters
By default, `RedshiftTracker` will capture the paths of your file and Redshift table, the column and row counts of the data being copied, and the schema of the data being copied. In addition to these metrics, additional metadata can be captured by passing the optional parameters below to `RedshiftTrackerConfig` as part of your `RedshiftTracker` integration:
* `with_preview=True`: Display a sample of your data in Databand (approximately 10-20 records).
* `with_stats=True`: Calculate column-level statistics for the data in motion. This includes but is not limited to metrics such as null counts and percentages, distinct counts, and statistical values such as the mean, standard deviation, and quartiles. 
* `with_partition=True`: If your file path includes partitioning such as `/date=20220415/`, you can use this parameter to ignore partitions in the parsing of your dataset names. This will help ensure that datasets across runs that have different partitioning will still be evaluated as the same dataset for the sake of trends and alerts. 

# Viewing Redshift Operations in Databand
Operations logged by `RedshiftTracker` will result in two datasets: the read from the file and the write to the Redshift table. Metrics related to these datasets such as the row counts, schemas, and column-level stats can be viewed on both the Affected Datasets tab of your pipeline run as well as the Datasets page in your Databand environment.

### Affected Datasets Tab
[block:image]
{
  "images": [
    {
      "image": [
        "https://files.readme.io/a3eac5f-Screen_Shot_2022-04-28_at_3.27.40_PM.png",
        "Screen Shot 2022-04-28 at 3.27.40 PM.png",
        1367,
        177,
        "#404141"
      ],
      "caption": ""
    }
  ]
}
[/block]
### Datasets Page
[block:image]
{
  "images": [
    {
      "image": [
        "https://files.readme.io/a6d166b-Screen_Shot_2022-04-28_at_3.24.30_PM.png",
        "Screen Shot 2022-04-28 at 3.24.30 PM.png",
        2151,
        705,
        "#383838"
      ],
      "caption": ""
    }
  ]
}
[/block]
# Limitations
Nested queries are not supported by `RedshiftTracker` (e.g. COPY (SELECT * FROM TABLE) table FROM...).