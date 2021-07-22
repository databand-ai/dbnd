package ai.databand.config;

import ai.databand.RandomNames;
import ai.databand.schema.AirflowTaskContext;
import ai.databand.schema.AzkabanTaskContext;
import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_DAG_ID;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_EXECUTION_DATE;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_TASK_ID;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_TRY_NUMBER;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_UID;
import static ai.databand.DbndPropertyNames.DBND__CORE__DATABAND_ACCESS_TOKEN;
import static ai.databand.DbndPropertyNames.DBND__CORE__DATABAND_URL;
import static ai.databand.DbndPropertyNames.DBND__LOG__PREVIEW_HEAD_BYTES;
import static ai.databand.DbndPropertyNames.DBND__LOG__PREVIEW_TAIL_BYTES;
import static ai.databand.DbndPropertyNames.DBND__RUN__NAME;
import static ai.databand.DbndPropertyNames.DBND__RUN__JOB_NAME;
import static ai.databand.DbndPropertyNames.DBND__SPARK__IO_TRACKING_ENABLED;
import static ai.databand.DbndPropertyNames.DBND__SPARK__LISTENER_INJECT_ENABLED;
import static ai.databand.DbndPropertyNames.DBND__TRACKING;
import static ai.databand.DbndPropertyNames.DBND__TRACKING__DATA_PREVIEW;
import static ai.databand.DbndPropertyNames.DBND__TRACKING__LOG_VALUE_PREVIEW;
import static ai.databand.DbndPropertyNames.DBND__TRACKING__VERBOSE;

/**
 * Databand configuration.
 */
public class DbndConfig implements PropertiesSource {

    private static final Logger LOG = LoggerFactory.getLogger(DbndConfig.class);

    private final AirflowTaskContext afCtx;
    private final AzkabanTaskContext azkbnCtx;
    private final boolean previewEnabled;
    private final boolean trackingEnabled;
    private final boolean verbose;
    private final String databandUrl;
    private final String cmd;
    private final String runName;
    private final Map<String, String> props;

    private static final int PREVIEW_HEAD_TAIL_DEFAULT = 32 * 1024;

    /**
     * Default override order, from higher priority to lowest:
     * 1. Spark config
     * 2. Process environment variables
     * 3. Java process system properties
     */
    public DbndConfig() {
        this(
            new SparkConf(
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

        previewEnabled = isTrue(this.props, DBND__TRACKING__DATA_PREVIEW) || isTrue(this.props, DBND__TRACKING__LOG_VALUE_PREVIEW);
        verbose = isTrue(this.props, DBND__TRACKING__VERBOSE);
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

    public boolean isVerbose() {
        return verbose;
    }

    public Optional<AirflowTaskContext> airflowContext() {
        return Optional.ofNullable(afCtx);
    }

    public Optional<AzkabanTaskContext> azkabanContext() {
        return Optional.ofNullable(azkbnCtx);
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

    public Optional<String> personalAccessToken() {
        return getValue(DBND__CORE__DATABAND_ACCESS_TOKEN);
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

    public boolean sparkListenerInjectEnabled() {
        return !isFalse(DBND__SPARK__LISTENER_INJECT_ENABLED);
    }

    public boolean sparkIoTrackingEnabled() {
        return isTrue(DBND__SPARK__IO_TRACKING_ENABLED);
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
            SparkContext ctx = SparkContext.getOrCreate();
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
}
