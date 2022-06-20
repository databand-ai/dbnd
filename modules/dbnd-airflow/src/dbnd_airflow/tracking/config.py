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
        default=False,
        description="Allow saving the results of tracked airflow operator",
    )[bool]

    track_xcom_values = parameter(
        default=False,
        description="Allow logging the values of xcom variables from airflow",
    )[bool]

    max_xcom_length = parameter(
        default=10, description="The amount of xcom values to track, per operator"
    )[int]

    # This shouldn't be set to None. In webserver None evaluates to True.
    af_with_monitor = parameter(
        default=True, description="Activate when airflow monitor is not in use"
    )[bool]

    sql_reporting = parameter(
        default=False, description="Enable targets reporting from sql queries"
    )[bool]


class TrackingSparkConfig(Config):
    _conf__task_family = "tracking_spark"

    provide_databand_service_endpoint = parameter(
        default=True,
        description="Should Databand inject tracker URL and access token into spark-submit cmd,"
        "e.g. `spark-submit --conf DBND__CORE__DATABAND_URL=... --conf DBND__CORE__DATABAND_ACCESS_TOKEN=... script.py",
    )
