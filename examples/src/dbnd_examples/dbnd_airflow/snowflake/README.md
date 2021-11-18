# Snowflake Examples

The following `snowflake_dag.py` file contains 3 dag examples of dbnd usage with snowflake database.
In All the examples, we are querying snowflake database, logging  and reporting metrics to databand server.

There are three ways one can integrate with dbnd tracking system when using snowflake with airflow:
1. Example 1 - Using dbnd's `DbndSnowflakeHook` custom hook.
2. Example 2 - Using airflow's official `SnowflakeHook` with dbnd's `snowflake_run()` run function.
3. Example 3 - Using airflow's official `SnowflakeHook` and dbnd's `LogSnowflakeTableOperator` and `LogSnowflakeResourceOperator` and pass them `query_id` and `session_id` via xcom.


## Example 1
In the first example, we are using our custom `DbndSnowflakeHook` in order to have customized snowflake interface.
Having it configured, we are running `log_snowflake_table` and `log_snowflake_resource_usage` from `dbnd_snowflake` module,
enables us to verbosely query snowflake db and log performance metadata respectively.


## Example 2
The second example is very similar to the first one, but instead of using our custom hook, we use `SnowflakeHook`, executing it with `snowflake_run()` from `dbnd_snowflake` module.
This example is airflow agnostic in terms that it doesn't require any custom dbnd operators, neither dbnd hooks.

## Example 3
In the third example we define operator for each command, `LogSnowflakeTableOperator` and `LogSnowflakeResourceOperator` from `dbnd_snowflake` module, and airflow's native `SnowflakeHook`.
The communication between the operators is based on xcom, providing relevant `session_id` and `query_id`.


