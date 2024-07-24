/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.config;

import ai.databand.RandomNames;
import ai.databand.schema.AirflowTaskContext;
import ai.databand.schema.AzkabanTaskContext;
import ai.databand.schema.DatabandTaskContext;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static ai.databand.DbndPropertyNames.*;

/**
 * Databand configuration.
 */
public class DbndConfig implements PropertiesSource {

    private static final Logger LOG = LoggerFactory.getLogger(DbndConfig.class);

    private final AirflowTaskContext afCtx;
    private final AzkabanTaskContext azkbnCtx;
    private final DatabandTaskContext dbndCtx;
    private final String fallbackTraceId;
    private final boolean previewEnabled;
    private boolean trackingEnabled;
    private final String databandUrl;
    private final String cmd;
    private final String runName;
    private final Map<String, String> props;

    //Not collecting any logs by default
    private static final int PREVIEW_HEAD_TAIL_DEFAULT = 0;
    /**
     * Default override order, from higher priority to lowest:
     * 1. Spark config
     * 2. Process environment variables
     * 3. Java process system properties
     */
    public DbndConfig() {
        this(
            new DbndSparkConf(
                new Env(
                    new JavaOpts()
                )
            )
        );
    }

    public DbndConfig(PropertiesSource props) {
        this(props, System.getProperties().getProperty("sun.java.command"));
    }

    public DbndConfig(PropertiesSource props, String cmd) {
        this.cmd = cmd;
        this.props = props.values();

        afCtx = buildAirflowCtxFromEnv(this.props);
        azkbnCtx = buildAzkabanCtxFromEnv(this.props);
        dbndCtx = buildDatabandCtxFromEnv(this.props);

        // used if no dbndCtx available
        fallbackTraceId = UUID.randomUUID().toString();

        previewEnabled = isTrue(this.props, DBND__TRACKING__DATA_PREVIEW) || isTrue(this.props, DBND__TRACKING__LOG_VALUE_PREVIEW);
        databandUrl = this.props.getOrDefault(DBND__CORE__DATABAND_URL, "http://localhost:8080");
        // tracking should be explicitly opt int when we're not running inside Airflow
        trackingEnabled = afCtx != null
            ? !isFalse(this.props, DBND__TRACKING) || isMissing(this.props, DBND__TRACKING)
            : isTrue(this.props, DBND__TRACKING);
        runName = azkbnCtx == null ? this.props.getOrDefault(DBND__RUN__NAME, RandomNames.next()) : azkbnCtx.jobRunName();
    }

    private AirflowTaskContext buildAirflowCtxFromEnv(Map<String, String> env) {
        if (env.containsKey(AIRFLOW_CTX_DAG_ID)
            && env.containsKey(AIRFLOW_CTX_EXECUTION_DATE)
            && env.containsKey(AIRFLOW_CTX_TASK_ID)
            && env.containsKey(AIRFLOW_CTX_TRY_NUMBER)) {

            return new AirflowTaskContext(
                env.get(AIRFLOW_CTX_UID),
                env.get(AIRFLOW_CTX_UID),
                env.get(AIRFLOW_CTX_DAG_ID),
                env.get(AIRFLOW_CTX_EXECUTION_DATE),
                env.get(AIRFLOW_CTX_TASK_ID),
                env.get(AIRFLOW_CTX_TRY_NUMBER)
            );
        } else {
            return null;
        }
    }

    private DatabandTaskContext buildDatabandCtxFromEnv(Map<String, String> env) {
        if (env.containsKey(DBND_ROOT_RUN_UID)
            && env.containsKey(DBND_PARENT_TASK_RUN_UID)
            && env.containsKey(DBND_PARENT_TASK_RUN_ATTEMPT_UID)) {
            return new DatabandTaskContext(
                env.get(DBND_ROOT_RUN_UID),
                env.get(DBND_PARENT_TASK_RUN_UID),
                env.get(DBND_PARENT_TASK_RUN_ATTEMPT_UID),
                env.getOrDefault(DBND_TRACE_ID, UUID.randomUUID().toString())
            );
        } else {
            return null;
        }
    }

    private AzkabanTaskContext buildAzkabanCtxFromEnv(Map<String, String> env) {
        Optional<AzkabanTaskContext> fromFile = readFromProperties(env);
        return fromFile.orElseGet(() -> buildFromMap(env));
    }

    private AzkabanTaskContext buildFromMap(Map<String, String> env) {
        if (env.containsKey("azkaban.flow.flowid")
            && env.containsKey("azkaban.flow.flowid")
            && env.containsKey("azkaban.flow.uuid")
            && env.containsKey("azkaban.flow.execid")
            && env.containsKey("azkaban.job.id")) {

            return new AzkabanTaskContext(
                env.get("azkaban.flow.flowid"),
                env.get("azkaban.flow.flowid"),
                env.get("azkaban.flow.uuid"),
                env.get("azkaban.flow.execid"),
                env.get("azkaban.job.id"),
                this
            );
        } else {
            return null;
        }
    }

    private Optional<AzkabanTaskContext> readFromProperties(Map<String, String> env) {
        if (env.containsKey("JOB_PROP_FILE")) {
            String fileName = env.get("JOB_PROP_FILE");
            try (FileInputStream input = new FileInputStream(fileName)) {
                Properties props = new Properties();
                props.load(input);
                return Optional.ofNullable(buildFromMap((Map) props));
            } catch (IOException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    public String databandUrl() {
        return databandUrl;
    }

    public boolean isPreviewEnabled() {
        return previewEnabled;
    }

    public boolean isTrackingEnabled() {
        return trackingEnabled;
    }

    public void setTrackingEnabled(boolean trackingEnabled) {
        this.trackingEnabled = trackingEnabled;
    }

    public Optional<AirflowTaskContext> airflowContext() {
        return Optional.ofNullable(afCtx);
    }

    public Optional<AzkabanTaskContext> azkabanContext() {
        return Optional.ofNullable(azkbnCtx);
    }

    public Optional<DatabandTaskContext> databandTaskContext() {
        return Optional.ofNullable(dbndCtx);
    }

    public String getTraceId() {
        if (this.databandTaskContext().isPresent()) {
            return this.databandTaskContext().get().getTraceId();
        }
        return fallbackTraceId;
    }

    public String cmd() {
        return cmd;
    }

    public String runName() {
        return runName;
    }

    public Optional<String> jobName() {
        return getValue(DBND__RUN__JOB_NAME);
    }

    public Optional<String> projectName() {
        return getValue(DBND__TRACKING__PROJECT);
    }

    public Optional<String> csrfToken() {
        return getValue(DBND__CSRF_TOKEN);
    }

    public Optional<String> sessionCookie() {
        return getValue(DBND__SESSION_COOKIE);
    }

    public Optional<String> personalAccessToken() {
        return getValue(DBND__CORE__DATABAND_ACCESS_TOKEN);
    }

    public boolean isVerbose() {
        return isTrue(DBND__VERBOSE);
    }

    public int previewTotalBytes() {
        return previewHeadBytes() + previewTailBytes();
    }

    public int previewHeadBytes() {
        return getInteger(DBND__LOG__PREVIEW_HEAD_BYTES, PREVIEW_HEAD_TAIL_DEFAULT);
    }

    public int previewTailBytes() {
        return getInteger(DBND__LOG__PREVIEW_TAIL_BYTES, PREVIEW_HEAD_TAIL_DEFAULT);
    }

    public boolean isSendingLogs() {
        return previewHeadBytes() > 0 || previewTailBytes() > 0;
    }

    protected Integer getInteger(String key, Integer defaultValue) {
        String value = props.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOG.error("Unable to read integer value from {}. Returning default value {}", value, defaultValue);
            return defaultValue;
        }
    }

    public String sparkAppName() {
        // todo: detect we're running inside spark
        try {
            SparkSession session = SparkSession.active();
            SparkContext ctx = session.sparkContext();
            return ctx.getConf().get("spark.app.name");
        } catch (Exception e) {
            return "none";
        }
    }

    public Map<String, String> values() {
        return Collections.unmodifiableMap(props);
    }

    protected final boolean isTrue(String key) {
        return Boolean.TRUE.toString().equalsIgnoreCase(props.get(key));
    }

    protected final boolean isFalse(String key) {
        return Boolean.FALSE.toString().equalsIgnoreCase(props.get(key));
    }

    protected final boolean isTrue(Map<String, String> env, String key) {
        return Boolean.TRUE.toString().equalsIgnoreCase(env.get(key));
    }

    protected final boolean isFalse(Map<String, String> env, String key) {
        return Boolean.FALSE.toString().equalsIgnoreCase(env.get(key));
    }

    protected final boolean isMissing(Map<String, String> env, String key) {
        return !env.containsKey(key);
    }

    @Override
    public Optional<String> getValue(String key) {
        String value = props.get(key);
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    /**
     * Mask sensitive config values.
     *
     * @param key
     * @param value
     * @return
     */
    protected String maskValue(String key, String value) {
        if (value == null) {
            return null;
        }
        if (key.toLowerCase().contains("token")) {
            return "***";
        }
        return value;
    }

    @Override
    public String toString() {
        if (props == null || props.isEmpty()) {
            return "{}";
        }
        return "\n" + props.keySet().stream()
            .filter(key -> key.toLowerCase().startsWith("dbnd") || key.toLowerCase().startsWith("airflow"))
            .map(key -> key + "=" + maskValue(key, props.get(key)))
            .collect(Collectors.joining("\n"));
    }
}
