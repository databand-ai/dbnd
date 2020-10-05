from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator

from dbnd._core.commands.metrics import log_snowflake_table
from dbnd_snowflake import log_snowflake_resource_usage


def _log_snowflake_table(
    table,
    snowflake_conn_id,
    warehouse=None,
    database=None,
    role=None,
    schema=None,
    account=None,
    key=None,
):
    hook = SnowflakeHook(
        snowflake_conn_id=snowflake_conn_id,
        warehouse=warehouse,
        account=account,
        database=database,
        role=role,
        schema=schema,
    )
    connection_string = hook.get_uri()
    return log_snowflake_table(table, connection_string, database, schema, key=key)


def _log_snowflake_resources(
    query_text,
    snowflake_conn_id,
    session_id=None,
    warehouse=None,
    database=None,
    role=None,
    schema=None,
    account=None,
    key=None,
):
    hook = SnowflakeHook(
        snowflake_conn_id,
        warehouse=warehouse,
        account=account,
        database=database,
        role=role,
        schema=schema,
    )
    conn = hook.get_uri()
    conn_params = hook._get_conn_params()
    log_snowflake_resource_usage(
        query_text,
        database=hook.database,
        user=conn_params["user"],
        connection_string=conn,
        session_id=session_id,
        key=key,
    )


def LogSnowflakeTableOperator(
    table,
    snowflake_conn_id="snowflake_default",
    warehouse=None,
    database=None,
    role=None,
    schema=None,
    account=None,
    key=None,
    *args,
    **kwargs
):
    return PythonOperator(
        *args,
        python_callable=_log_snowflake_table,
        op_kwargs=dict(
            snowflake_conn_id=snowflake_conn_id,
            table=table,
            warehouse=warehouse,
            account=account,
            database=database,
            role=role,
            schema=schema,
            key=key,
        ),
        **kwargs
    )


def log_snowflake_operator(op: SnowflakeOperator, table, **kwargs):
    task_id = kwargs.pop("task_id", "log_table_%s_%s" % (op.task_id, table))
    return LogSnowflakeTableOperator(
        table=table,
        snowflake_conn_id=kwargs.pop("snowflake_conn_id", op.snowflake_conn_id),
        database=kwargs.pop("database", op.database),
        warehouse=kwargs.pop("warehouse", op.warehouse),
        role=kwargs.pop("role", op.role),
        schema=kwargs.pop("schema", op.schema),
        task_id=task_id,
        **kwargs
    )


def LogSnowflakeResourceOperator(
    query_text,
    snowflake_conn_id="snowflake_default",
    session_id=None,
    warehouse=None,
    database=None,
    role=None,
    schema=None,
    account=None,
    key=None,
    *args,
    **kwargs
):
    return PythonOperator(
        *args,
        python_callable=_log_snowflake_resources,
        op_kwargs=dict(
            snowflake_conn_id=snowflake_conn_id,
            query_text=query_text,
            session_id=session_id,
            warehouse=warehouse,
            account=account,
            database=database,
            role=role,
            schema=schema,
            key=key,
        ),
        **kwargs
    )


def log_snowflake_resource_operator(op: SnowflakeOperator, **kwargs):
    task_id = kwargs.pop("task_id", "log_resources_%s" % (op.task_id))
    return LogSnowflakeResourceOperator(
        query_text=op.sql,
        snowflake_conn_id=kwargs.pop("snowflake_conn_id", op.snowflake_conn_id),
        database=kwargs.pop("database", op.database),
        warehouse=kwargs.pop("warehouse", op.warehouse),
        role=kwargs.pop("role", op.role),
        schema=kwargs.pop("schema", op.schema),
        task_id=task_id,
        **kwargs
    )
