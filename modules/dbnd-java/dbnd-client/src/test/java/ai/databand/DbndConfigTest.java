package ai.databand;

import ai.databand.config.DbndConfig;
import ai.databand.config.SimpleProps;
import ai.databand.config.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_DAG_ID;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_EXECUTION_DATE;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_TASK_ID;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_TRY_NUMBER;
import static ai.databand.DbndPropertyNames.DBND__CORE__DATABAND_URL;
import static ai.databand.DbndPropertyNames.DBND__TRACKING;
import static ai.databand.DbndPropertyNames.DBND__TRACKING__LOG_VALUE_PREVIEW;
import static org.hamcrest.MatcherAssert.assertThat;

class DbndConfigTest {

    @Test
    public void testTrackingEnabledNonAf() {
        checkTrackingEnabled(
            "Tracking should be enabled when env variable DBND__TRACKING set to True and we're running not in airflow",
            Collections.singletonMap(DBND__TRACKING, "True"),
            true
        );

        checkTrackingEnabled(
            "Tracking should be disabled when env variable DBND__TRACKING set to False",
            Collections.singletonMap(DBND__TRACKING, "False"),
            false
        );

        checkTrackingEnabled(
            "Tracking should be disabled when env variable DBND__TRACKING is empty and we're running not in airflow context",
            Collections.emptyMap(),
            false
        );

        SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.env.DBND__TRACKING", "True")
            .getOrCreate();

        checkTrackingEnabled(
            "Tracking should be enabled when spark conf variable DBND__TRACKING set to True and we're running not in airflow",
            Collections.singletonMap(DBND__TRACKING, "False"),
            true
        );

        sparkSession.stop();


        sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.env.DBND__TRACKING", "False")
            .getOrCreate();

        checkTrackingEnabled(
            "Tracking should be disabled when spark conf variable DBND__TRACKING set to False",
            Collections.singletonMap(DBND__TRACKING, "True"),
            false
        );

        sparkSession.stop();
    }

    @Test
    public void testTrackingEnabledAf() {
        Map<String, String> afContext = new HashMap<>(1);
        afContext.put(AIRFLOW_CTX_DAG_ID, "process_data");
        afContext.put(AIRFLOW_CTX_EXECUTION_DATE, "2020-02-02");
        afContext.put(AIRFLOW_CTX_TASK_ID, "run");
        afContext.put(AIRFLOW_CTX_TRY_NUMBER, "1");

        checkTrackingEnabled(
            "Tracking should be enabled when env variable DBND__TRACKING is not set to True and we're running in airflow",
            afContext,
            true
        );

        Map<String, String> afContextWithDisabledTracking = new HashMap<>(afContext);
        afContextWithDisabledTracking.put(DBND__TRACKING, "False");
        checkTrackingEnabled(
            "Tracking should be disabled when env variable DBND__TRACKING set to False and we're running in airflow",
            afContextWithDisabledTracking,
            false
        );
    }

    protected void checkTrackingEnabled(String reason, Map<String, String> env, Object expectedValue) {
        DbndConfig conf = new DbndConfig(new SparkConf(new SimpleProps(env)));
        assertThat(reason, conf.isTrackingEnabled(), Matchers.equalTo(expectedValue));
    }

    @Test
    public void testPreviewEnabled() {
        checkPreviewEnabled(
            "Preview should be enabled when env variable DBND__TRACKING__LOG_VALUE_PREVIEW set to True",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "True"),
            true
        );

        SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW", "True")
            .getOrCreate();

        checkPreviewEnabled(
            "Preview should be enabled when spark conf variable DBND__TRACKING__LOG_VALUE_PREVIEW set to True",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "False"),
            true
        );

        sparkSession.stop();

        sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW", "False")
            .getOrCreate();

        checkPreviewEnabled(
            "Preview should be disabled when env variable DBND__TRACKING__LOG_VALUE_PREVIEW set to False",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "False"),
            false
        );

        sparkSession.stop();

        sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW", "False")
            .getOrCreate();

        checkPreviewEnabled(
            "Preview should be disabled when spark conf variable DBND__TRACKING__LOG_VALUE_PREVIEW set to False",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "True"),
            false
        );

        sparkSession.stop();
    }

    protected void checkPreviewEnabled(String reason, Map<String, String> env, Object expectedValue) {
        DbndConfig conf = new DbndConfig(new SparkConf(new SimpleProps(env)));
        assertThat(reason, conf.isPreviewEnabled(), Matchers.equalTo(expectedValue));
    }

    @Test
    public void testPipelineRunName() {
        SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.env.DBND__RUN__NAME", "pipeline_run")
            .getOrCreate();
        DbndConfig conf = new DbndConfig(new SparkConf(new SimpleProps()));
        assertThat("Wrong pipeline run name", conf.runName(), Matchers.equalTo("pipeline_run"));
        sparkSession.stop();
    }

    @Test
    public void testDatabandUrl() {
        checkDatabandUrl(
            "Databand URL should be set to http://localhost:8080 when no env variable is present",
            Collections.emptyMap(),
            "http://localhost:8080"
        );

        SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW", "False")
            .getOrCreate();

        checkDatabandUrl(
            "Databand URL should be read form env variables",
            Collections.singletonMap(DBND__CORE__DATABAND_URL, "https://tracker.databand.ai"),
            "https://tracker.databand.ai"
        );

        sparkSession.stop();

        sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.env.DBND__CORE__DATABAND_URL", "https://tracker2.databand.ai")
            .getOrCreate();

        checkDatabandUrl(
            "Databand URL should be read form spark conf variables when no env variable is present",
            Collections.singletonMap(DBND__CORE__DATABAND_URL, "https://tracker.databand.ai"),
            "https://tracker2.databand.ai"
        );

        sparkSession.stop();
    }

    protected void checkDatabandUrl(String reason, Map<String, String> env, Object expectedValue) {
        DbndConfig conf = new DbndConfig(new SparkConf(new SimpleProps(env)));
        assertThat(reason, conf.databandUrl(), Matchers.equalTo(expectedValue));
    }

}
