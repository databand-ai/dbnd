# © Copyright Databand.ai, an IBM Company 2022

"""
class TestDocTrackingPandasDataframe:
    def test_doc(self):
        # Python 3.6.8
        from dbnd import log_metric, log_dataframe
        import pandas as pd

        transactions_df = pd.read_csv("~/data/example.csv")

        # log dataframe
        log_dataframe(
            "Full table", transactions_df, with_histograms=False, with_preview=False
        )

        # log_metrics
        minimum_amount = 5000
        num_large_transactions = transactions_df[
            transactions_df["transaction_amt"] >= minimum_amount
        ].shape[0]
        avg_large_transaction = transactions_df[
            transactions_df["transaction_amt"] >= minimum_amount
        ].mean()["transaction_amt"]
        large_transactions_df = transactions_df[
            transactions_df["transaction_amt"] >= minimum_amount
        ]

        log_metric("Number of large transactions(>= 5000)", num_large_transactions)
        log_metric("mean large transactions", avg_large_transaction)
        log_dataframe(
            "Large transactions (>= 5000)", large_transactions_df, with_preview=False
        )



    def test_process_customers_data(self):
        #### DOC START
        @task
        def prepare_data(data: pd.DataFrame = "s3://pipelines/customers_data.csv") -> pd.DataFrame:
            log_dataframe("customers_data", data,
                          with_histograms=LogDataRequest(include_all_numeric=True,
                                                         exclude_columns=["name", "phone"]))
        #### DOC END
        prepare_data.dbnd_run()
"""
