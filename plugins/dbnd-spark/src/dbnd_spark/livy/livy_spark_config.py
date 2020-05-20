from dbnd import parameter
from dbnd_spark.spark_config import SparkEngineConfig


class LivySparkConfig(SparkEngineConfig):
    # we don't want spark class to inherit from this one, as it should has Config behaviour
    _conf__task_family = "livy"
    _conf__help_description = "livy configuration"

    url = parameter(description="livy connection url (for example: http://livy:8998)")[
        str
    ]

    def get_spark_ctrl(self, task_run):
        from dbnd_spark.livy.livy_spark import LivySparkCtrl

        return LivySparkCtrl(task_run)
