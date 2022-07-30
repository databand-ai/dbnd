# © Copyright Databand.ai, an IBM Company 2022

'''
class TestDocTrackingSnowflakeGuide:
    def test_doc(self):
        #### DOC START
        import snowflake.connector
        from dbnd_snowflake import log_snowflake_resource_usage, log_snowflake_table

        def establish_connection():
            """Establish and return a Snowflake Connection using Snowflake Python Connector"""
            snowflake_conn_id = "snowflake_connection"
            snowflake_connection = snowflake.connector.connect(
                user=your_username,
                password=your_password,
                account=your_full_account_name,
            )
            return snowflake_connection

        def run_query(connection, query):
            """Generate a cursor, and execute a query. Returns the query result, query ID and session ID."""
            cursor = connection.cursor()
            query_results = cursor.execute(query).fetchall()
            query_id, session_id = cursor.sfqid, cursor.connection.session_id
            cursor.close()
            return query_results, query_id, session_id

        def query_snowflake():
            """Query Snowflake, track resource usage, and log Snowflake table"""
            connection_string = f"snowflake://your_connection_string"
            snowflake_connection = establish_connection()
            query_ids = []

            database = "SNOWFLAKE_SAMPLE_DATA"
            schema = "TPCDS_SF100TCL"
            table_name = "STORE_SALES"
            query = f'select * from "{database}"."{schema}"."{table_name}" limit 30'

            results, query_id, session_id = run_query(snowflake_connection, query)
            query_ids.append(query_id)

            log_snowflake_resource_usage(
                database=database,
                connection_string=connection_string,
                query_ids=query_ids,
                session_id=int(session_id),
                delay=1,
            )

            log_snowflake_table(
                table_name=f"{table_name}",
                connection_string=connection_string,
                database=database,
                schema=schema,
                with_preview=False,
            )

            # As best practice, close the Snowflake connection
            snowflake_connection.close()

        if __name__ == "__main__":
            query_snowflake()
        #### DOC END
        query_snowflake()
'''
