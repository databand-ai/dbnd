'''
class TestDocTrackingSql:
    def test_doc(self):
        #### DOC START
        # Python 3.6.8
        from sqlalchemy import create_engine
        from dbnd import log_metric

        conn_string = 'postgresql+psycopg2://dbnd_postgres:postgres@localhost/dbnd_postgres'
        pg_db = create_engine(conn_string)

        minimum_amount = 5000

        # logging count of rows where transaction_amt >= 5000
        num_large_transactions_query = f"""SELECT COUNT(transaction_amt) FROM transactions_data WHERE transaction_amt >= {minimum_amount};"""
        results = pg_db.execute(num_large_transactions_query)
        num_large_transactions = results.fetchone()[0]
        log_metric("Number of large transactions(>= 5000)", num_large_transactions)

        # logging average of rowsw where transaction_amt >= 5000
        avg_large_transactions_query = f"""SELECT AVG(transaction_amt) FROM transactions_data WHERE transaction_amt >= {minimum_amount};"""
        results = pg_db.execute(avg_large_transactions_query)
        avg_large_transactions = results.fetchone()[0]
        log_metric("mean large transactions", avg_large_transactions)

        # logging all rows where transaction_amt >= 5000
        large_transactions_query = f"""SELECT * FROM transactions_data WHERE transaction_amt >= {minimum_amount};"""
        results = pg_db.execute(large_transactions_query)
        large_transactions = [row for row in results]
        log_metric("large transactions rows", large_transactions)

        # logging table shape
        num_rows_query = f"""SELECT COUNT (*) FROM transactions_data"""
        results = pg_db.execute(num_rows_query)
        num_rows = results.fetchone()[0]
        num_cols_query = f"""SELECT COUNT(column_name) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'transactions_data';"""
        results = pg_db.execute(num_cols_query)
        num_cols = results.fetchone()[0]

        log_metric("table shape", (num_rows, num_cols))
        #### DOC END
'''
