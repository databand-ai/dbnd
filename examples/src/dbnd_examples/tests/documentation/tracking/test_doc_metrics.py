import pytest

from dbnd_examples.data import data_repo


pandas_df = data_repo.wines


class TestDocMetrics:
    def test_doc(self):
        from dbnd import task, output, parameter, LogDataRequest
        import pandas as pd
        from pandas import DataFrame

        #### DOC START
        from dbnd import log_metric, log_dataframe

        # define a function with key-value and dataframe metrics

        @task
        def prepare_data(data: pd.DataFrame, length: int, rows: int):
            log_metric("rows", rows)
            log_metric("length", length)
            log_dataframe("data", data)
            return "OK"

        log_dataframe(
            "data",
            pandas_df,
            with_preview=True,
            with_size=True,
            with_schema=True,
            with_stats=True,
            with_histograms=True,
        )

        log_dataframe("data", pandas_df, with_stats=True, with_histograms=True)

        #### DOC END

    def test_prepare_data_task_decorator(self):
        from dbnd import task, output, parameter, LogDataRequest
        import pandas as pd
        from pandas import DataFrame

        #### DOC START
        @task(raw_data=parameter(log_preview=False, log_histograms=True)[DataFrame])
        def read_data(raw_data: pd.DataFrame, length: int):
            pass

        @task(result=output(log_schema=True, log_histograms=True)[DataFrame])
        def prepare_data(data: pd.DataFrame, length: int):
            return data

        #### DOC END

        read_data.dbnd_run(raw_data=data_repo.wines, length=5)
        prepare_data.dbnd_run(data=data_repo.wines, length=5)

    @pytest.mark.skip()
    def test_prepare_data_with_histograms(self):
        import pandas as pd
        from dbnd import task, log_dataframe, LogDataRequest

        #### DOC START
        @task
        def prepare_data(
            data: pd.DataFrame = "s3://pipelines/customers_data.csv",
        ) -> pd.DataFrame:
            log_dataframe(
                "customers_data",
                data,
                with_histograms=LogDataRequest(
                    include_all_numeric=True, exclude_columns=["name", "phone"]
                ),
            )

        #### DOC END

        prepare_data.dbnd_run()

    def test_calculate_alpha(self):
        import sys
        from operator import add
        from pyspark.sql import SparkSession
        from dbnd import log_metric, task

        @task
        def calculate_counts(input_file, output_file):
            spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

            lines = spark.read.text(input_file).rdd.map(lambda r: r[0])
            counts = (
                lines.flatMap(lambda x: x.split(" "))
                .map(lambda x: (x, 1))
                .reduceByKey(add)
            )
            counts.saveAsTextFile(output_file)
            output = counts.collect()
            for (word, count) in output:
                print("%s: %i" % (word, count))
            log_metric("counts", len(output))
            spark.sparkContext.stop()

        if __name__ == "__main__":
            if len(sys.argv) != 3:
                print("Usage: wordcount  ")
                sys.exit(-1)
            calculate_counts(sys.argv[1], sys.argv[2])
