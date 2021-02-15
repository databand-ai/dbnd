# Template Airflow DAGs for Monitoring

The files listed in this directory are template Airflow DAGs to easily integrate `dbnd` monitoring your pipelines. All of these templates can be modified to better suite the needs of your pipelines or data.

## Contents
- AWS Redshift
    - Redshift Cluster Monitoring
    - Redshift Table Monitoring

## AWS Redshift
- [Redshift Cluster Monitoring](./redshift_cluster_template_dag.py)
    - Template includes the following metrics:
        - Number of tables in cluster
        - Minimum number of rows in cluster tables
        - Maximum number of rows in cluster tables
        - Mean number of rows in cluster tables
        - Median number of rows in cluster tables
        - Shape of all tables in tables (columns, rows)
        - Disk usage of cluster (Capacity, Free, and Used in GB)
    - To use this template DAG, specify the following environment variables: 
        ```bash
        export REDSHIFT_CONNECTION_ID="<your Redshift connection ID>"
        export REDSHIFT_CLUSTER_NAME="<your Redshift Cluster name>"
        export REDSHIFT_TABLE_MONITOR_SCHEDULE="<cron format schedule to run monitor>"

        # optional variable, default will be public schema: 
        export REDSHIFT_SCHEMA="<target Redshift schema to monitor>"
        ```
- [Redshift Table Monitoring](./redshift_table_template_dag.py)
    - Template includes the following metrics:
        - Record count of the target table
        - `Null`/`NaN` record count for each column the target table
        - Duplicate Record count (all columns match)
        - Minimum of `numeric` columns in the target table
        - Maximum of `numeric` columns in the target table
        - Mean of `numeric` columns in the target table
        - Median of `numeric` columns in the target table
    - To use this template DAG, specify the following environment variables: 
        ```bash
        export REDSHIFT_CONNECTION_ID="<your Redshift connection ID>"
        export REDSHIFT_MONITOR_TARGET_TABLE="<target Redshift table name>"
        export REDSHIFT_TABLE_MONITOR_SCHEDULE="<cron format schedule to run monitor>"

        # optional variable, default will be last 1000 rows
        export REDSHIFT_MONITOR_TABLE_LIMIT="<number of rows to monitor>"
        ```
