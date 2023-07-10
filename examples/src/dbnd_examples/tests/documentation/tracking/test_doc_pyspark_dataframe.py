# © Copyright Databand.ai, an IBM Company 2022


class TestDocPysparkDataframe:
    def test_doc(self):
        #### DOC START
        import sys

        from operator import add

        from pyspark.sql import SparkSession

        from dbnd import log_metric, task

        @task
        def prepare_data(data, output_file):
            spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

            lines = spark.read.text(data).rdd.map(lambda r: r[0])
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
            prepare_data(sys.argv[1], sys.argv[2])
        #### DOC END
        # prepare_data(data_repo.vegetables, "~/Data/example")
