"""
class TestDocSparkOrchestration:
    def test_prepare_data_spark_task(self):
        #### DOC START
        import pyspark.sql as spark
        from dbnd import output, parameter
        from dbnd_spark.spark import spark_task

        @spark_task
        def prepare_data_spark(text) -> spark.DataFrame:
            from operator import add
            from dbnd_spark.spark import get_spark_session

            lines = text.rdd.map(lambda r: r[0])
            counts = (
                lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
            )

            return counts
        #### DOC END
        prepare_data_spark.dbnd_run(text = "Hello!")


    def test_prepare_data_pyspark_task(self):
        #### DOC START
        class PrepareDataPySparkTask(PySparkTask):
            text = parameter.data
            counters = output

            python_script = spark_folder("pyspark/word_count.py")

            def application_args(self):
                return [self.text, self.counters]
        #### DOC END
        PrepareDataPySparkTask(text=data_repo.wines.dbnd_run())

    def test_prepare_data_jvm_spark_task(self):
        #### DOC START
        class PrepareDataSparkTask(SparkTask):
            text = parameter.data
            counters = output

            main_class = "ai.databand.examples.WordCount"
            defaults = {
                SparkConfig.main_jar: "${DBND_HOME}/databand_examples/tool_spark/spark_jvm/target/ai.databand.examples-1.0-SNAPSHOT.jar"}

            def application_args(self):
                return [self.text, self.counters]
        #### DOC END

        PrepareDataSparkTask(text=data_repo.wines).dbnd_run()"""
