"""
class TestDocTrackingRedshift:
    def test_doc(self):
        #### DOC START
        import psycopg2
        from dbnd_redshift.dbnd_redshift import get_redshift_query_id, log_redshift_resource_usage

        def establish_connection():
            '''establish and return a Redshift connection'''
            redshift_conn = psycopg2.connect(
                dbname='database',
                host='cluster-name.acc-name.region.redshift.amazonaws.com',
                port='5439', user='user', password='password'
            )
            return redshift_conn

        def execute_and_log(redshift_connection, query):
            '''execute a query and log Redshift resource usage'''
            cursor = redshift_connection.cursor()
            cursor.execute(query)

            query_id = get_redshift_query_id(query, connection=redshift_connection)
            log_redshift_resource_usage(query_id, connection=redshift_connection)

        if __name__ == "__main__":
            redshift_connection = establish_connection()

            query = "SELECT * FROM transaction_data WHERE transaction_amt >= 5000;"
            execute_and_log(redshift_connection, query)
            print("works")
        #### DOC END
        """
