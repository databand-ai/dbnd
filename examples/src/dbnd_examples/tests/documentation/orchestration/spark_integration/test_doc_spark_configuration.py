class TestDocSparkConfiguration:
    def test_doc(self):
        """
        #### DOC START
        class PrepareData(SparkTask):
            text = parameter.data
            counters = output

            main_class = "org.predict_wine_quality.PrepareData"
            # overides value of SparkConfig object
            defaults = {SparkConfig.driver_memory: "2.5g", "spark.executor_memory": "1g"}

            def application_args(self):
                return [self.text, self.counters]
        #### DOC END
        PrepareData.dbnd_run(text="Hello!")"""
