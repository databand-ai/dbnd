import os

import airflow

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from numpy import mean, median
from pandas import DataFrame

from dbnd import log_metric


# get environment variables:
REDSHIFT_CONNECTION_ID = os.getenv("REDSHIFT_CONN", default="redshift_conn")
REDSHIFT_CLUSTER_NAME = os.getenv("REDSHIFT_CLUSTER_NAME", default="redshift_cluster")
REDSHIFT_CLUSTER_MONITOR_SCHEDULE = os.getenv(
    "REDSHIFT_CLUSTER_MONITOR_SCHEDULE", default="@daily"
)
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", default="public")

# Queries:
COUNT_TABLES = """
select count(*)
from information_schema.tables
where table_schema = %s;
"""

COUNT_TABLE_ROWS = """
select svv.table_name,
       table_info.tbl_rows as row_count
from svv_tables svv
join svv_table_info table_info
          on svv.table_name = table_info.table
where svv.table_schema = %s
    and svv.table_type = 'BASE TABLE';
"""

DESCRIBE_TABLES = """
select * from pg_table_def where schemaname = %s;
"""

DISK_USAGE = """
select
  round(sum(capacity),2)/1024 as capacity_gb,
  round (sum(used),2)/1024 as used_gb,
  round((sum(capacity) - sum(used)),2)/1024 as free_gb
from
  stv_partitions where part_begin=0;
"""

DEFAULT_ARGS = {
    "owner": "databand",
    "start_date": airflow.utils.dates.days_ago(0),
    "provide_context": False,
}


def monitor_redshift(**op_kwarg):
    """Redshift database monitor collects the following metrics:
    - Number of tables in database
    - Shape of each table in the database
    - Min, max, mean, median number of rows across all tables,
    - Min, max, mean, median number of columns across all tables,
    - Total number of rows and columns
    - Largest tables by row and column
    - Disk capacity, Free space on disk, Used space on disk (in GB)
    - Disk percent usage
    """
    hook = PostgresHook(REDSHIFT_CONNECTION_ID)
    num_redshift_tables = hook.get_first(COUNT_TABLES, parameters=[REDSHIFT_SCHEMA])[0]
    log_metric("Cluster table count", num_redshift_tables)

    table_row_counts = hook.get_records(COUNT_TABLE_ROWS, parameters=[REDSHIFT_SCHEMA])
    num_rows_per_table = {}
    for tablename, row_count in table_row_counts:
        num_rows_per_table[tablename] = int(round(row_count))

    row_counts = list(num_rows_per_table.values())
    log_metric("Max table row count", max(row_counts))
    log_metric("Min table row count", min(row_counts))
    log_metric("Mean table row count", round(mean(row_counts), 2))
    log_metric("Median table row count", median(row_counts))

    tables = hook.get_pandas_df(DESCRIBE_TABLES, parameters=[REDSHIFT_SCHEMA])
    table_shapes = DataFrame()
    table_shapes["columns"] = tables.groupby("tablename").nunique("column")["column"]
    table_shapes["tablename"] = tables["tablename"].unique()
    table_shapes["rows"] = (
        table_shapes["tablename"].map(num_rows_per_table).fillna(0).astype(int)
    )

    for _, row in table_shapes.iterrows():
        log_metric("{} shape".format(row["tablename"]), (row["columns"], row["rows"]))

    log_metric("Max table column count", table_shapes["columns"].max())
    log_metric("Min table column count", table_shapes["columns"].max())
    log_metric("Mean table column count", round(table_shapes["columns"].mean(), 2))
    log_metric("Median table column count", table_shapes["columns"].median())

    log_metric("Total columns", table_shapes["columns"].sum())
    log_metric("Total rows", table_shapes["rows"].sum())

    max_row_table = table_shapes[table_shapes["rows"] == table_shapes["rows"].max()]
    max_col_table = table_shapes[
        table_shapes["columns"] == table_shapes["columns"].max()
    ]
    log_metric("Largest table (by row count)", max_row_table["tablename"][0])
    log_metric("Largest table (by col count)", max_col_table["tablename"][0])

    disk_stats = hook.get_records(DISK_USAGE).pop()
    disk_capacity, disk_used, disk_free = disk_stats
    log_metric("Disk capacity (GB)", disk_capacity)
    log_metric("Disk used (GB)", disk_used)
    log_metric("Disk free (GB)", disk_free)
    log_metric("Percent Disk usage", round((disk_used / disk_capacity) * 100, 2))


with DAG(
    dag_id="dbnd_{}_monitor".format(REDSHIFT_CLUSTER_NAME),
    schedule_interval="{}".format(REDSHIFT_CLUSTER_MONITOR_SCHEDULE),
    default_args=DEFAULT_ARGS,
) as dbnd_template_dag:

    redshift_monitor = PythonOperator(
        task_id="monitor_redshift_cluster", python_callable=monitor_redshift
    )
