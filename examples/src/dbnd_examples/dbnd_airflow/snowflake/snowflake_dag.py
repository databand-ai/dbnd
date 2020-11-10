import airflow

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.models import DAG, BaseOperator

from dbnd_snowflake import log_snowflake_resource_usage, log_snowflake_table
from dbnd_snowflake.airflow_hooks import DbndSnowflakeHook, snowflake_run
from dbnd_snowflake.airflow_operators import (
    LogSnowflakeResourceOperator,
    LogSnowflakeTableOperator,
)


args = {"start_date": airflow.utils.dates.days_ago(2), "owner": "databand"}

snowflake_conn_id = "test_snowflake_conn"
database = "SNOWFLAKE_SAMPLE_DATA"
schema = "TPCDS_SF100TCL"
table = "CUSTOMER"
select_query = f"select * from {database}.{schema}.{table} limit 1000"


dag_examples = [
    DAG(
        dag_id="snowflake_example_dag_{}".format(i),
        default_args=args,
        schedule_interval="0 1 * * *",  # Daily
    )
    for i in range(1, 4)
]
for dag in dag_examples:
    globals()[dag.dag_id] = dag


# EXAMPLE 1: Custom Hook Example
class Example1Operator(BaseOperator):
    def execute(self, context):
        hook = DbndSnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        session_id, query_ids = hook.run(sql=select_query)
        connection_string = hook.get_uri()

        log_snowflake_table(
            table_name=table,
            connection_string=connection_string,
            database=database,
            schema=schema,
            key=f"example1.{table}",
            with_preview=False,
            raise_on_error=False,
        )

        log_snowflake_resource_usage(
            database=database,
            key=f"example1.res.{session_id}{query_ids[0]}",
            connection_string=connection_string,
            query_ids=query_ids,
            session_id=int(session_id),
            raise_on_error=True,
        )


with dag_examples[0]:
    ex1_task = Example1Operator(task_id="example1")


# EXAMPLE 2: snowflake_run Example
class Example2Operator(BaseOperator):
    def execute(self, context):
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        session_id, query_ids = snowflake_run(hook, select_query)
        connection_string = hook.get_uri()

        log_snowflake_table(
            table_name=table,
            connection_string=connection_string,
            database=database,
            schema=schema,
            key=f"example1.{table}",
            with_preview=False,
            raise_on_error=False,
        )

        log_snowflake_resource_usage(
            database=database,
            key=f"example1.{session_id}{query_ids[0]}",
            connection_string=connection_string,
            query_ids=query_ids,
            session_id=int(session_id),
            raise_on_error=True,
        )


with dag_examples[1]:
    ex2_task = Example2Operator(task_id="example2")


# EXAMPLE 3: pass session_id, query_id over xcom to another operator
class Example3Operator(BaseOperator):
    def execute(self, context):
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        session_id, query_id = snowflake_run(hook, select_query)

        context["ti"].xcom_push(key="session_id", value=session_id)
        context["ti"].xcom_push(key="query_id", value=query_id)


with dag_examples[2]:
    ex3_task = Example3Operator(task_id="example3")
    log_snowflake_table_task = LogSnowflakeTableOperator(
        table=table,
        snowflake_conn_id=snowflake_conn_id,
        warehouse=None,
        database=database,
        schema=schema,
        task_id="log_snowflake_table_task",
        key=f"snowflake_table_{table}",
    )

    log_snowflake_resources_task = LogSnowflakeResourceOperator(
        session_id="{{ti.xcom_pull(key='session_id') }}",
        query_id="{{ti.xcom_pull(key='query_id')[0] }}",
        snowflake_conn_id=snowflake_conn_id,
        warehouse=None,
        database=database,
        schema=schema,
        task_id="log_snowflake_resources_task",
        key="snowflake_query_resources",
    )
    ex3_task >> log_snowflake_table_task >> log_snowflake_resources_task
