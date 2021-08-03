package ai.databand;

public abstract class DbndPropertyNames {

    /**
     * Databand tracker URL.
     */
    public static final String DBND__CORE__DATABAND_URL = "dbnd.core.databand_url";

    /**
     * Databand tracker personal access token.
     */
    public static final String DBND__CORE__DATABAND_ACCESS_TOKEN = "dbnd.core.databand_access_token";

    /**
     * Tracking enabled flag.
     */
    public static final String DBND__TRACKING = "dbnd.tracking";

    /**
     * Tracking enabled flag;
     */
    public static final String DBND__TRACKING__ENABLED = "dbnd.tracking.enabled";

    /**
     * Turn on verbose logging for tracking requests.
     */
    public static final String DBND__TRACKING__VERBOSE = "dbnd.tracking.verbose";

    /**
     * Turn of rich data preview (dataframes and histograms).
     *
     * @deprecated use DBND__TRACKING__LOG_VALUE_PREVIEW instead
     */
    @Deprecated
    public static final String DBND__TRACKING__DATA_PREVIEW = "dbnd.tracking.data_preview";

    /**
     * Calculate and log value preview. Can be expensive on Spark.
     */
    public static final String DBND__TRACKING__LOG_VALUE_PREVIEW = "dbnd.tracking.log_value_preview";

    /**
     * Max head size of the log file, bytes to be sent to server. Default 32KiB.
     */
    public static final String DBND__LOG__PREVIEW_HEAD_BYTES = "dbnd.log.preview_head_bytes";

    /**
     * Max tail size of the log file, bytes to be sent to server. Default 32KiB.
     */
    public static final String DBND__LOG__PREVIEW_TAIL_BYTES = "dbnd.log.preview_tail_bytes";

    /**
     * Turn on spark listener auto-injection.
     */
    public static final String DBND__SPARK__LISTENER_INJECT_ENABLED = "dbnd.spark.listener_inject_enabled";

    /**
     * Turn on advanced Spark I/O tracking.
     */
    public static final String DBND__SPARK__IO_TRACKING_ENABLED = "dbnd.spark.io_tracking_enabled";

    /**
     * Override run name.
     */
    public static final String DBND__RUN__JOB_NAME = "dbnd.run.job_name";

    /**
     * Override run name.
     */
    public static final String DBND__RUN__NAME = "dbnd.run.name";

    /**
     * List of Azkaban projects to sync. If not specified, all projects will be synced.
     */
    public static final String DBND__AZKABAN__SYNC_PROJECTS = "dbnd.azkaban.sync_projects";

    /**
     * List of Azkaban flows to sync. If not specified, all flows will be synced.
     */
    public static final String DBND__AZKABAN__SYNC_FLOWS = "dbnd.azkaban.sync_flows";

    /**
     * Airflow context env variables.
     */
    public static final String AIRFLOW_CTX_UID = "AIRFLOW_CTX_UID";
    public static final String AIRFLOW_CTX_DAG_ID = "AIRFLOW_CTX_DAG_ID";
    public static final String AIRFLOW_CTX_EXECUTION_DATE = "AIRFLOW_CTX_EXECUTION_DATE";
    public static final String AIRFLOW_CTX_TASK_ID = "AIRFLOW_CTX_TASK_ID";
    public static final String AIRFLOW_CTX_TRY_NUMBER = "AIRFLOW_CTX_TRY_NUMBER";

    /**
     * Databand context env variables.
     */
    public static final String DBND_ROOT_RUN_UID = "dbnd_root_run_uid";
    public static final String DBND_PARENT_TASK_RUN_UID = "dbnd_parent_task_run_uid";
    public static final String DBND_PARENT_TASK_RUN_ATTEMPT_UID = "dbnd_parent_task_run_attempt_uid";
}
