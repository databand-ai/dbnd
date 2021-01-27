from typing import List

from dbnd import parameter
from dbnd._core.task import Config


class AirflowTrackingConfig(Config):
    _conf__task_family = "airflow_tracking"

    spark_submit_dbnd_java_agent = parameter(
        default=None,
        description="A dbnd java agent jar which used to track a java application, located on the local machine",
    )[str]

    databricks_dbnd_java_agent = parameter(
        default=None,
        description="A dbnd java agent jar which used to track a java application, located on remote machine",
    )[str]

    track_airflow_execute_result = parameter(
        default=True,
        description="Allow saving the results of tracked airflow operator",
    )[bool]

    track_xcom_values = parameter(
        default=True,
        description="Allow logging the values of xcom variables from airflow",
    )[bool]

    max_xcom_size = parameter(
        default=10000,
        description="Maximum size, in bytes, of single xcom value that we will track",
    )[int]

    max_xcom_length = parameter(
        default=10, description="The amount of xcom values to track, per operator"
    )[int]
