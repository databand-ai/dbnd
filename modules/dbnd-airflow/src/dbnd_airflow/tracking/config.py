# © Copyright Databand.ai, an IBM Company 2022

from dbnd import parameter
from dbnd._core.task import Config


class AirflowTrackingConfig(Config):
    _conf__task_family = "airflow_tracking"

    spark_submit_dbnd_java_agent = parameter(
        default=None,
        description="Set the DBND Java agent `jar` which will be "
        "used to track a Java application, located on the local machine",
    )[str]

    databricks_dbnd_java_agent = parameter(
        default=None,
        description="Set the DBND Java agent `jar` which will be "
        "used to track a Java application, located on remote machine.",
    )[str]

    track_airflow_execute_result = parameter(
        default=False,
        description="Enable saving the results of tracked Airflow operators.",
    )[bool]

    track_xcom_values = parameter(
        default=False,
        description="Enable logging the values of xcom variables from airflow.",
    )[bool]

    max_xcom_length = parameter(
        default=10, description="Set the number of xcom values to track, per operator."
    )[int]

    # This shouldn't be set to None. In webserver None evaluates to True.
    af_with_monitor = parameter(
        default=True, description="Activate when airflow monitor is not in use."
    )[bool]

    sql_reporting = parameter(
        default=False, description="Enable targets reporting from SQL queries."
    )[bool]


class TrackingSparkConfig(Config):
    _conf__task_family = "tracking_spark"

    provide_databand_service_endpoint = parameter(
        default=True,
        description="Should Databand inject tracker URL and access token into spark-submit cmd,"
        "e.g. `spark-submit --conf DBND__CORE__DATABAND_URL=... --conf DBND__CORE__DATABAND_ACCESS_TOKEN=... script.py",
    )[bool]

    agent_path = parameter(
        default=None,
        description="Path to Databand Agent jar to be added to the spark job as java agent. Jar file has to be placed directly to the cluster filesystem.",
    )[str]

    jar_path = parameter(
        default=None,
        description="Path to Databand Agent jar to be added to the spark job as regular jar file. Jar file can be placed to the S3/GCS/DBFS as well as directly to the cluster filesystem.",
    )[str]

    query_listener = parameter(
        default=False,
        description="Should Databand turn on Spark Query Listener and automatically collect datasource operations",
    )[bool]

    def spark_conf(self):
        result = {}
        if self.agent_path:
            result["spark.driver.extraJavaOptions"] = "-javaagent:{agent_path}".format(
                agent_path=self.agent_path
            )
        if self.jar_path:
            result["spark.jars"] = self.jar_path
        if self.query_listener:
            result[
                "spark.sql.queryExecutionListeners"
            ] = "ai.databand.spark.DbndSparkQueryExecutionListener"
        return result
