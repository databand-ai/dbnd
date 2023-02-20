# © Copyright Databand.ai, an IBM Company 2022

from dbnd import parameter
from dbnd._core.utils.http.constants import NO_AUTH
from dbnd_spark.spark_config import SparkEngineConfig


class LivySparkConfig(SparkEngineConfig):
    # we don't want spark class to inherit from this one, as it should has Config behaviour
    _conf__task_family = "livy"
    _conf__help_description = "livy configuration"

    url = parameter(
        description="Determine livy's connection url, e.g. `http://livy:8998`"
    )[str]

    auth = parameter(
        description="Set livy auth, e.g. None, Kerberos, or Basic_Access",
        default=NO_AUTH,
    )[str]

    user = parameter(description="Set livy auth user.", default="")[str]

    password = parameter(description="Set livy auth password.", default="")[str]

    ignore_ssl_errors = parameter(
        description="Enable ignoring ssl errors.", default=False
    )[bool]

    job_submitted_hook = parameter(
        description="Set the user code to be run after livy batch submit. This is a reference to a function. "
        "The expected interface is `(LivySparkCtrl, Dict[str, Any]) -> None`",
        default=None,
    )[str]

    job_status_hook = parameter(
        description="Set the user code to be run at each livy batch status update. This is a reference to a function. "
        "The expected interface is `(LivySparkCtrl, Dict[str, Any]) -> None`",
        default=None,
    )[str]

    retry_on_status_error = parameter(
        description="Set the number of retries for http requests if the status code is not accepted.",
        default=20,
    )[int]

    retry_on_status_error_delay = parameter(
        description="Determien the amount of time, in seconds, in between retries for http requests.",
        default=15,
    )[int]

    def get_spark_ctrl(self, task_run):
        from dbnd_spark.livy.livy_spark import LivySparkCtrl

        return LivySparkCtrl(task_run)
