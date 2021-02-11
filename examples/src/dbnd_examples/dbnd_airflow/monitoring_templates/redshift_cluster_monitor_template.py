import os

import airflow

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from numpy import mean, median
from pandas import DataFrame

from dbnd import log_dataframe, log_metric
from dbnd._core.constants import DbndTargetOperationType


# Grab metadata about a redshift cluster
REDSHIFT_CONNECTION_ID = "redshift_conn"
REDSHIFT_CLUSTER_NAME = "redshift-cluster-1"
REDSHIFT_SCHEMA = "public"

COUNT_TABLES = f"""
select count(*)
from information_schema.tables
where table_schema = %s;
"""

COUNT_TABLE_ROWS = f"""
select svv.table_name,
       table_info.tbl_rows as row_count
from svv_tables svv
join svv_table_info table_info
          on svv.table_name = table_info.table
where svv.table_schema = %s
    and svv.table_type = 'BASE TABLE';
"""

DESCRIBE_TABLES = f"""
select * from pg_table_def where schemaname = %s;
"""

DISK_USAGE = f"""
select
  round(sum(capacity),2)/1024 as capacity_gb,
  round (sum(used),2)/1024 as used_gb,
  round((sum(capacity) - sum(used)),2)/1024 as free_gb
from
  stv_partitions where part_begin=0;
"""

DEFAULT_ARGS = {
    "owner": "databand",
    "start_date": airflow.utils.dates.days_ago(2),
    "provide_context": False,
}


def monitor_redshift(**op_kwarg):
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

    # get column metadata of all tables
    tables = hook.get_pandas_df(DESCRIBE_TABLES, parameters=[REDSHIFT_SCHEMA])

    table_shapes = DataFrame()
    table_shapes["columns"] = tables.groupby("tablename").nunique("column")["column"]
    table_shapes["tablename"] = tables["tablename"].unique()
    table_shapes["rows"] = table_shapes["tablename"].map(num_rows_per_table).fillna(0)

    for _, row in table_shapes.iterrows():
        log_metric(f"{row['tablename']} shape:", (row["columns"], row["rows"]))

    disk_stats = hook.get_records(DISK_USAGE).pop()
    disk_capacity, disk_used, disk_free = disk_stats
    log_metric("Disk capacity (GB)", disk_capacity)
    log_metric("Disk used (GB)", disk_used)
    log_metric("Disk free (GB)", disk_free)


with DAG(
    dag_id=f"dbnd_{REDSHIFT_CLUSTER_NAME}_monitor",
    schedule_interval="@daily",  # every 12 hours
    default_args=DEFAULT_ARGS,
    tags=["python", "redshift", "monitor"],
) as dbnd_template_dag:

    redshift_monitor = PythonOperator(
        task_id="monitor_redshift_cluster", python_callable=monitor_redshift
    )
