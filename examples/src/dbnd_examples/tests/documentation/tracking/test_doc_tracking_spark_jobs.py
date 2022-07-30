# © Copyright Databand.ai, an IBM Company 2022

import sys


class TestDocTrackingSparkJobs:
    def test_doc(self):
        #### DOC START
        import logging

        from operator import add

        from pyspark.sql import SparkSession

        from dbnd import log_metric, task

        logger = logging.getLogger(__name__)

        @task
        def prepare_data(data, output_file):
            spark = SparkSession.builder.appName("PrepareData").getOrCreate()

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
            logger.info("Log message from EMR cluster")
            spark.sparkContext.stop()

        if __name__ == "__main__":
            if len(sys.argv) != 3:
                print("Usage: wordcount  ")
                sys.exit(-1)
            prepare_data(sys.argv[1], sys.argv[2])
        #### DOC END
        # prepare_data.dbnd_run(data_repo.vegetables, "~/data/example")
