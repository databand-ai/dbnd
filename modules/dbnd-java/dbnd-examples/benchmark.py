# © Copyright Databand.ai, an IBM Company 2022

import os

from dbnd import parameter
from dbnd_spark import SparkTask
from dbnd_spark.spark_config import SparkConfig


class BenchmarkTask(SparkTask):
    run_inputs = parameter[str]
    run_method = parameter[str]
    run_option = parameter[str]

    main_class = "ai.databand.examples.PerformanceBenchmark"
    defaults = {
        SparkConfig.main_jar: os.path.join(
            os.getcwd(), "build", "libs", "dbnd-examples-latest-all.jar"
        ),
        SparkConfig.driver_memory: "2G",
    }

    def application_args(self):
        return [self.run_inputs, self.run_method, self.run_option]
