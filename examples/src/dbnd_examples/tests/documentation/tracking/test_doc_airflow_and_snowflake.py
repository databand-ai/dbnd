# © Copyright Databand.ai, an IBM Company 2022

"""
class TestDocAirflowAndSnowflake:
    def test_doc(self):
        #### DOC START
        import airflow

        from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
        from airflow.models import DAG, BaseOperator

        args = {"start_date": airflow.utils.dates.days_ago(2), "owner": "databand"}

        snowflake_conn_id = "test_snowflake_conn"
        database = "SNOWFLAKE_SAMPLE_DATA"
        schema = "TPCDS_SF100TCL"
        table = "CUSTOMER"
        select_query = f"select * from {database}.{schema}.{table} limit 1000"

        example_dags = [
            DAG(
                dag_id="example_dag_{}".format(i),
                default_args=args,
                schedule_interval="0 1 * * *",  # Daily
            )
            for i in range(1, 4)
        ]
        for dag in example_dags:
            globals()[dag.dag_id] = dag



        from dbnd_snowflake import log_snowflake_resource_usage, log_snowflake_table
        from dbnd_snowflake.airflow_hooks import DbndSnowflakeHook

        class CalculateAlpha(BaseOperator):
            def execute(self, context):
                hook = DbndSnowflakeHook(snowflake_conn_id=snowflake_conn_id)
                session_id, query_ids = hook.run(sql=select_query)
                connection_string = hook.get_uri()

                log_snowflake_table(
                    table_name=table,
                    connection_string=connection_string,
                    database=database,
                    schema=schema,
                    key=f"calculate_alpha.{table}",
                    with_preview=False,
                    raise_on_error=False,
                )

                log_snowflake_resource_usage(
                    database=database,
                    key=f"calculate_alpha.res.{session_id}{query_ids[0]}",
                    connection_string=connection_string,
                    query_ids=query_ids,
                    session_id=int(session_id),
                    raise_on_error=True,
                )

        with example_dags[0]:
            calculate_alpha = CalculateAlpha(task_id="calculate_alpha")



        from dbnd_snowflake import log_snowflake_resource_usage, log_snowflake_table
        from dbnd_snowflake.airflow_hooks import snowflake_run

        class CalculateBeta(BaseOperator):
            def execute(self, context):
                hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
                session_id, query_ids = snowflake_run(hook, select_query)
                connection_string = hook.get_uri()

                log_snowflake_table(
                    table_name=table,
                    connection_string=connection_string,
                    database=database,
                    schema=schema,
                    key=f"calculate_beta.{table}",
                    with_preview=False,
                    raise_on_error=False,
                )

                log_snowflake_resource_usage(
                    database=database,
                    key=f"calculate_beta.{session_id}{query_ids[0]}",
                    connection_string=connection_string,
                    query_ids=query_ids,
                    session_id=int(session_id),
                    raise_on_error=True,
                )

        with example_dags[1]:
            calculate_beta = CalculateBeta(task_id="calculate_beta")



        from dbnd_snowflake.airflow_hooks import snowflake_run
        from dbnd_snowflake.airflow_operators import (
            LogSnowflakeResourceOperator,
            LogSnowflakeTableOperator,
        )

        with example_dags[2]:
            calculate_coefficient = CalculateCoefficient(task_id="calculate_coefficient")
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
            calculate_coefficient >> log_snowflake_table_task >> log_snowflake_resources_task
        #### DOC END
"""
