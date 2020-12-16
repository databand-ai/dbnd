import os

from datetime import timedelta

import airflow

from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator

from dbnd_snowflake import snowflake_query_tracker
from dbnd_snowflake.airflow_operators import LogSnowflakeTableOperator


user = os.environ.get("SNOWFLAKE_USER")
account = os.environ.get("SNOWFLAKE_ACCOUNT")
database = "SNOWFLAKE_SAMPLE_DATA"
schema = "TPCDS_SF100TCL"
table = "CUSTOMER"
SNOWFLAKE_CONNECTION_ID = "test_snowflake_conn"

args = {"start_date": airflow.utils.dates.days_ago(2), "owner": "databand"}

select_query = f'select c_birth_year from "{database}"."{schema}"."{table}" where C_SALUTATION = "Dr." limit 1000'
# Update query is expected to fail
update_query = f'UPDATE "{database}"."{schema}"."{table}" SET C_CUSTOMER_ID=C_CUSTOMER_ID WHERE C_SALUTATION = "Dr."'


def process_records(records):
    return min(r[0] for r in records)


def process_customers(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION_ID)
    customers = snowflake_hook.get_records(select_query)

    # Process records
    process_records(customers)


def process_customers_with_monitoring(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION_ID)
    with snowflake_query_tracker(log_tables=False, database=database):
        customers = snowflake_hook.get_records(select_query)
    # Process records - Same code
    process_records(customers)


def update_customers(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION_ID)
    snowflake_hook.run(update_query)


def update_customers_with_monitoring(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION_ID)
    with snowflake_query_tracker(log_tables=False, database=database):
        snowflake_hook.run(update_query)


dag_no_dbnd = DAG(
    dag_id="snowflake_no_monitoring_dag",
    default_args=args,
    schedule_interval=timedelta(hours=6),
)

with dag_no_dbnd:
    get_customers = PythonOperator(
        task_id="get_customers_no_dbnd", python_callable=process_customers
    )
    update_db = PythonOperator(
        task_id="update_customers_no_dbnd", python_callable=update_customers
    )

    get_customers >> update_db

dag_with_dbnd = DAG(
    dag_id="snowflake_with_monitoring_dag",
    default_args=args,
    schedule_interval=timedelta(hours=6),
)

with dag_with_dbnd:
    get_customers_dbnd = PythonOperator(
        task_id="get_customers_with_dbnd",
        python_callable=process_customers_with_monitoring,
    )
    update_db_dbnd = PythonOperator(
        task_id="update_customers_no_dbnd",
        python_callable=update_customers_with_monitoring,
    )
    log_snowflake_table_task = LogSnowflakeTableOperator(
        table=table,
        snowflake_conn_id=SNOWFLAKE_CONNECTION_ID,
        warehouse=None,
        database=database,
        schema=schema,
        account=account,
        task_id="log_snowflake_table_task",
        key=f"snowflake_table_{table}",
        with_preview=True,
        with_schema=True,
        raise_on_error=True,
    )

    get_customers_dbnd >> update_db_dbnd >> log_snowflake_table_task
