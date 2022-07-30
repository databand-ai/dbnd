# © Copyright Databand.ai, an IBM Company 2022


class TestDocTrackingDatabricks:
    def test_doc(self):
        #### DOC START
        from random import randint
        from time import sleep

        import pandas as pd

        from dbnd import dataset_op_logger, dbnd_tracking, log_metric

        def execute():
            for int in range(randint(0, 10)):
                sleep(randint(0, 10))
                log_metric(f"interation_{int}", int)

            with dataset_op_logger("databricks://test/load/read", "read") as logger:
                data = {"row_1": [3, 2, 1, 0], "row_2": ["a", "b", "c", "d"]}
                read_df = pd.DataFrame.from_dict(data, orient="index")
                logger.set(data=read_df)

            with dataset_op_logger("databricks://test/load/write", "write") as logger:
                data = {"row_1": [3, 2, 1, 0], "row_2": ["a", "b", "c", "d"]}
                write_df = pd.DataFrame.from_dict(
                    data, orient="index", columns=["A", "B", "C", "D"]
                )
                logger.set(data=write_df)

        with dbnd_tracking("databricks_test"):
            execute()
        #### DOC END
