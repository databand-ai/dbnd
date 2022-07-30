# © Copyright Databand.ai, an IBM Company 2022

from dbnd import parameter
from dbnd._core.utils.http.constants import NO_AUTH
from dbnd_spark.spark_config import SparkEngineConfig


class LivySparkConfig(SparkEngineConfig):
    # we don't want spark class to inherit from this one, as it should has Config behaviour
    _conf__task_family = "livy"
    _conf__help_description = "livy configuration"

    url = parameter(description="livy connection url (for example: http://livy:8998)")[
        str
    ]

    auth = parameter(
        description="livy auth , support list are None, Kerberos, Basic_Access",
        default=NO_AUTH,
    )[str]

    user = parameter(description="livy auth , user", default="")[str]

    password = parameter(description="livy auth , password", default="")[str]

    ignore_ssl_errors = parameter(description="ignore ssl error", default=False)[bool]

    job_submitted_hook = parameter(
        description="User code to run after livy batch submit (a reference to a function)"
        "expected interface (LivySparkCtrl, Dict[str, Any]) -> None",
        default=None,
    )[str]

    job_status_hook = parameter(
        description="User code to run at each livy batch status update (a reference to a function)"
        "expected interface (LivySparkCtrl, Dict[str, Any]) -> None",
        default=None,
    )[str]

    retry_on_status_error = parameter(
        description="The number of retries for http requests if status code is not accepted",
        default=20,
    )[int]

    retry_on_status_error_delay = parameter(
        description="The amount of time in seconds between retries for http requests",
        default=15,
    )[int]

    def get_spark_ctrl(self, task_run):
        from dbnd_spark.livy.livy_spark import LivySparkCtrl

        return LivySparkCtrl(task_run)
